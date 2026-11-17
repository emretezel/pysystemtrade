"""
Tests for two safety fixes made after the 2026-11-17 US10 incident:

1. A contract order is only released from algo control (allowing another broker
   order to be created) once IB has confirmed the previous broker order is finished.
   ib_insync marks a trade Cancelled whenever an error message arrives carrying its
   order id, eg Error 201 when IB rejects a *change* to an order that is still working.

2. Broker fills that add up to more than the contract order wanted no longer raise
   out of the fills sweep and kill the stack handler.

None of these tests need an IB connection or a database.
"""
import asyncio
import datetime

import pytest
from ib_insync import Contract, Order, OrderStatus, Trade, TradeLogEntry

from sysbrokers.IB.client.ib_orders_client import is_same_ib_order
from sysbrokers.IB.ib_orders import (
    confirm_trade_is_finished_cancelling_if_required,
    trade_is_finished_according_to_broker,
)
from sysexecution.orders.base_orders import overFilledOrder
from sysexecution.stack_handler import fills as fills_module
from sysexecution.stack_handler.create_broker_orders_from_contract_orders import (
    stackHandlerCreateBrokerOrders,
)
from sysexecution.stack_handler.fills import stackHandlerForFills

NOW = datetime.datetime(2026, 11, 17, 15, 11, 35, tzinfo=datetime.timezone.utc)

# What the trade log looked like on 2026-11-17 when IB rejected the price change
ACKNOWLEDGED_LOG = [("PendingSubmit", "", 0), ("Submitted", "", 0)]
PHANTOM_CANCEL_LOG = ACKNOWLEDGED_LOG + [
    ("Submitted", "Modify", 0),
    (
        "Cancelled",
        "Error 201, reqId 2184: Order rejected - reason:Invalid Price",
        201,
    ),
]
CANCEL_CONFIRMED_BY_IB_LOG = ACKNOWLEDGED_LOG + [
    ("PendingCancel", "", 0),
    ("Cancelled", "", 0),
]


def make_trade(
    status: str,
    log_entries=(),
    order_id: int = 2184,
    client_id: int = 7696,
    perm_id: int = 1613996568,
) -> Trade:
    order = Order(
        orderId=order_id,
        clientId=client_id,
        permId=perm_id,
        action="BUY",
        totalQuantity=1,
        orderType="LMT",
        lmtPrice=0.171875,
    )
    order_status = OrderStatus(orderId=order_id, status=status)
    log = [
        TradeLogEntry(NOW, entry_status, message, error_code)
        for entry_status, message, error_code in log_entries
    ]

    return Trade(Contract(), order, order_status, [], log)


class FakeLog:
    def __init__(self):
        self.records = []

    def debug(self, msg, *args, **kwargs):
        self.records.append(("debug", msg))

    def warning(self, msg, *args, **kwargs):
        self.records.append(("warning", msg))

    def error(self, msg, *args, **kwargs):
        self.records.append(("error", msg))

    def critical(self, msg, *args, **kwargs):
        self.records.append(("critical", msg))

    def messages(self, level: str) -> list:
        return [msg for record_level, msg in self.records if record_level == level]


class FakeIB:
    def __init__(self, on_sleep=None):
        self.sleeps = []
        self._on_sleep = on_sleep

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        if self._on_sleep is not None:
            self._on_sleep()


class FakeIbClient:
    """
    open_answers: what IB says each time we ask 'is this order still open?'.
    The last answer is repeated for ever. An exception instance is raised.
    """

    def __init__(self, open_answers=(False,), on_cancel=None, on_sleep=None):
        self.ib = FakeIB(on_sleep=on_sleep)
        self.cancelled_orders = []
        self.open_queries = 0
        self._open_answers = list(open_answers)
        self._on_cancel = on_cancel

    def refresh(self):
        pass

    def ib_cancel_order(self, order):
        self.cancelled_orders.append(order)
        if self._on_cancel is not None:
            self._on_cancel()

    def ib_is_trade_listed_as_open(self, trade) -> bool:
        self.open_queries += 1
        if len(self._open_answers) > 1:
            answer = self._open_answers.pop(0)
        else:
            answer = self._open_answers[0]
        if isinstance(answer, BaseException):
            raise answer

        return answer


def confirm(ib_client, trade, log, wait_time_seconds=0.05):
    return confirm_trade_is_finished_cancelling_if_required(
        ib_client=ib_client,
        trade=trade,
        log=log,
        wait_time_seconds=wait_time_seconds,
        poll_seconds=0,
        second_look_seconds=0,
    )


# ---------------------------------------------------------------------------
# Is the trade finished, according to IB's own messages?
# ---------------------------------------------------------------------------


def test_filled_trade_is_finished():
    trade = make_trade("Filled", ACKNOWLEDGED_LOG + [("Filled", "", 0)])
    assert trade_is_finished_according_to_broker(trade) is True


def test_cancel_confirmed_by_ib_is_finished():
    trade = make_trade("Cancelled", CANCEL_CONFIRMED_BY_IB_LOG)
    assert trade_is_finished_according_to_broker(trade) is True


def test_cancelled_by_an_error_message_is_not_finished():
    # This is the bug: the price change was rejected, the order is still working
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)
    assert trade_is_finished_according_to_broker(trade) is False


def test_real_cancel_after_a_phantom_one_is_finished():
    log = PHANTOM_CANCEL_LOG + [
        ("Submitted", "", 0),
        ("PendingCancel", "", 0),
        ("Cancelled", "", 0),
    ]
    trade = make_trade("Cancelled", log)
    assert trade_is_finished_according_to_broker(trade) is True


def test_cancelled_with_no_evidence_in_log_is_not_finished():
    trade = make_trade("Cancelled", [])
    assert trade_is_finished_according_to_broker(trade) is False


@pytest.mark.parametrize(
    "status",
    [
        "PendingSubmit",
        "ApiPending",
        "PreSubmitted",
        "Submitted",
        "PendingCancel",
        "Inactive",
    ],
)
def test_any_other_status_is_not_finished(status):
    trade = make_trade(status, ACKNOWLEDGED_LOG)
    assert trade_is_finished_according_to_broker(trade) is False


def test_nothing_remaining_as_reported_by_ib_is_finished():
    # fills can arrive a moment before the 'Filled' status does
    trade = make_trade("Submitted", ACKNOWLEDGED_LOG)
    trade.orderStatus.filled = 1.0
    trade.orderStatus.remaining = 0.0
    assert trade_is_finished_according_to_broker(trade) is True


def test_unfilled_order_with_default_status_fields_is_not_finished():
    # a brand new OrderStatus has filled=0 and remaining=0: must not look finished
    trade = make_trade("Submitted", ACKNOWLEDGED_LOG)
    assert trade.orderStatus.filled == 0
    assert trade.orderStatus.remaining == 0
    assert trade_is_finished_according_to_broker(trade) is False


# ---------------------------------------------------------------------------
# Matching one of IB's open orders to our trade
# ---------------------------------------------------------------------------


def test_same_order_matched_on_identity_permid_or_client_and_order_id():
    trade = make_trade("Submitted")
    assert is_same_ib_order(trade, trade)

    same_permid = make_trade("Submitted", order_id=1, client_id=1)
    assert is_same_ib_order(same_permid, trade)

    same_client_and_order_id = make_trade("Submitted", perm_id=0)
    assert is_same_ib_order(same_client_and_order_id, trade)


def test_different_orders_are_not_matched():
    trade = make_trade("Submitted")

    other = make_trade("Submitted", order_id=2192, perm_id=1613996569)
    assert not is_same_ib_order(other, trade)

    other_client_same_order_id = make_trade("Submitted", client_id=1, perm_id=0)
    assert not is_same_ib_order(other_client_same_order_id, trade)

    no_ids_at_all = make_trade("Submitted", order_id=0, perm_id=0)
    also_no_ids = make_trade("Submitted", order_id=0, perm_id=0)
    assert not is_same_ib_order(no_ids_at_all, also_no_ids)


# ---------------------------------------------------------------------------
# Confirm finished, cancelling if required
# ---------------------------------------------------------------------------


def test_finished_trade_needs_no_cancel_and_no_query():
    trade = make_trade("Filled", ACKNOWLEDGED_LOG + [("Filled", "", 0)])
    ib_client = FakeIbClient()
    log = FakeLog()

    assert confirm(ib_client, trade, log) is True
    assert ib_client.cancelled_orders == []
    assert ib_client.open_queries == 0
    assert log.records == []


def test_phantom_cancel_is_really_cancelled_then_confirmed():
    # 2026-11-17: marked Cancelled by Error 201 but still working at IB
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)
    ib_client = FakeIbClient(open_answers=[True, False])
    log = FakeLog()

    assert confirm(ib_client, trade, log) is True
    assert ib_client.cancelled_orders == [trade.order]
    # listed, then not listed twice running: one reply is not enough
    assert ib_client.open_queries == 3
    assert len(log.messages("warning")) == 1


def test_order_that_never_goes_away_is_not_confirmed():
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)
    ib_client = FakeIbClient(open_answers=[True])
    log = FakeLog()

    assert confirm(ib_client, trade, log) is False
    assert ib_client.cancelled_orders == [trade.order]
    assert ib_client.open_queries >= 1


def test_one_reply_not_listing_the_order_is_not_enough():
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)
    # missing from one (perhaps incomplete) reply, then listed again for ever
    ib_client = FakeIbClient(open_answers=[False, True])
    log = FakeLog()

    assert confirm(ib_client, trade, log) is False


@pytest.mark.parametrize(
    "no_answer", [asyncio.TimeoutError(), asyncio.CancelledError(), ConnectionError()]
)
def test_ib_not_answering_counts_as_still_open(no_answer):
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)
    ib_client = FakeIbClient(open_answers=[no_answer])
    log = FakeLog()

    assert confirm(ib_client, trade, log) is False
    assert ib_client.cancelled_orders == [trade.order]
    assert ib_client.open_queries >= 1


def test_no_answer_between_two_replies_resets_the_count():
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)
    ib_client = FakeIbClient(
        open_answers=[False, asyncio.TimeoutError(), False, asyncio.TimeoutError(), True]
    )
    log = FakeLog()

    assert confirm(ib_client, trade, log) is False


def test_not_listed_but_still_held_as_working_is_not_confirmed():
    # IB doesn't list it, yet nothing has told us it is filled or cancelled
    trade = make_trade("Submitted", ACKNOWLEDGED_LOG)
    ib_client = FakeIbClient(open_answers=[False])
    log = FakeLog()

    assert confirm(ib_client, trade, log) is False
    assert ib_client.cancelled_orders == [trade.order]


def test_ib_insync_marks_a_working_order_cancelled_when_a_change_is_rejected():
    # Pins the ib_insync behaviour this fix exists for. If this ever fails after
    # an upgrade, the library has changed and the fix should be looked at again
    from ib_insync import IB

    ib = IB()
    trade = make_trade("Submitted", ACKNOWLEDGED_LOG)
    ib.wrapper.clientId = trade.order.clientId
    ib.wrapper.trades[(trade.order.clientId, trade.order.orderId)] = trade

    ib.wrapper.error(
        trade.order.orderId, 201, "Order rejected - reason:Invalid Price", ""
    )

    assert trade.orderStatus.status == "Cancelled"
    assert trade.log[-1].errorCode == 201
    assert trade_is_finished_according_to_broker(trade) is False


def test_live_order_is_cancelled_and_ib_confirmation_is_enough():
    trade = make_trade("Submitted", ACKNOWLEDGED_LOG)

    def ib_confirms_cancel():
        trade.orderStatus.status = "Cancelled"
        trade.log.append(TradeLogEntry(NOW, "Cancelled", "", 0))

    ib_client = FakeIbClient(open_answers=[True], on_cancel=ib_confirms_cancel)
    log = FakeLog()

    assert confirm(ib_client, trade, log) is True
    assert ib_client.cancelled_orders == [trade.order]
    assert ib_client.open_queries == 0


def test_status_already_on_its_way_is_given_a_moment_to_arrive():
    trade = make_trade("Submitted", ACKNOWLEDGED_LOG)

    def filled_status_arrives():
        trade.orderStatus.status = "Filled"

    ib_client = FakeIbClient(on_sleep=filled_status_arrives)
    log = FakeLog()

    assert confirm(ib_client, trade, log) is True
    assert ib_client.cancelled_orders == []
    assert ib_client.open_queries == 0
    assert log.records == []


def test_failure_to_send_cancel_does_not_raise():
    trade = make_trade("Cancelled", PHANTOM_CANCEL_LOG)

    def cancel_blows_up():
        raise ConnectionError("Not connected")

    ib_client = FakeIbClient(open_answers=[True], on_cancel=cancel_blows_up)
    log = FakeLog()

    assert confirm(ib_client, trade, log) is False


def test_replay_of_incident_using_real_ib_insync_state_changes():
    """
    Drive a real ib_insync wrapper (not connected) through what happened on 2026-11-17,
    then check the order is cancelled for real and only then reported as finished
    """
    from ib_insync import IB

    ib = IB()
    trade = make_trade("PendingSubmit", [("PendingSubmit", "", 0)])
    order = trade.order
    ib.wrapper.clientId = order.clientId
    ib.wrapper.trades[(order.clientId, order.orderId)] = trade

    def ib_sends_order_status(status: str):
        ib.wrapper.orderStatus(
            order.orderId, status, 0.0, 1.0, 0.0, order.permId, 0, 0.0, order.clientId, ""
        )

    # IB acknowledges the order: it is working
    ib_sends_order_status("Submitted")
    assert trade.orderStatus.status == "Submitted"

    # IB rejects a change of limit price. The order is still working at IB...
    ib.wrapper.error(order.orderId, 201, "Order rejected - reason:Invalid Price", "")
    # ...but ib_insync now says Cancelled, which is what the algo saw and believed
    assert trade.orderStatus.status == "Cancelled"

    def ib_accepts_our_cancel():
        ib_sends_order_status("Cancelled")

    ib_client = FakeIbClient(open_answers=[False], on_cancel=ib_accepts_our_cancel)
    log = FakeLog()

    assert confirm(ib_client, trade, log) is True
    # we did send a real cancel
    assert ib_client.cancelled_orders == [order]
    # IB's own 'Cancelled' message changed nothing ib_insync records, so the log can't
    # tell us the cancel was confirmed: that is why we also ask IB what is open, twice
    assert trade.log[-1].errorCode == 201
    assert ib_client.open_queries == 2


# ---------------------------------------------------------------------------
# The wiring between the stack handler and the function above
# ---------------------------------------------------------------------------


def test_ib_execution_stack_passes_the_trade_and_client_through():
    from sysbrokers.IB.ib_orders import ibExecutionStackData
    from sysbrokers.IB.ib_translate_broker_order_objects import tradeWithContract

    trade = make_trade("Cancelled", CANCEL_CONFIRMED_BY_IB_LOG)
    fake_client = FakeIbClient()
    fake_log = FakeLog()

    class ExecutionStackUnderTest(ibExecutionStackData):
        def __init__(self):
            pass

        @property
        def ib_client(self):
            return fake_client

        @property
        def log(self):
            return fake_log

    class OrderWithControlsStandIn:
        control_object = tradeWithContract(None, trade)
        order = FakeBrokerOrder()

    execution_stack = ExecutionStackUnderTest()
    result = execution_stack.confirm_order_is_finished_cancelling_if_required(
        OrderWithControlsStandIn()
    )

    assert result is True
    assert fake_client.cancelled_orders == []


def test_data_broker_passes_the_question_to_the_execution_stack():
    from sysproduction.data.broker import dataBroker

    asked_about = []

    class ExecutionStackStandIn:
        def confirm_order_is_finished_cancelling_if_required(self, order_with_controls):
            asked_about.append(order_with_controls)
            return False

    class DataBrokerUnderTest(dataBroker):
        def __init__(self):
            pass

        @property
        def broker_execution_stack_data(self):
            return ExecutionStackStandIn()

    order_with_controls = object()
    result = DataBrokerUnderTest().confirm_order_is_finished_cancelling_if_required(
        order_with_controls
    )

    assert result is False
    assert asked_about == [order_with_controls]


# ---------------------------------------------------------------------------
# Stack handler: only release the contract order if the broker order is finished
# ---------------------------------------------------------------------------


class FakeBrokerOrder:
    def __init__(self, order_id: int = 6811, parent: int = 4966):
        self.order_id = order_id
        self.parent = parent

    def log_with_attributes(self, log):
        return log

    def __repr__(self):
        return "FakeBrokerOrder(%d)" % self.order_id


class FakeOrderWithControls:
    def __init__(self, update_raises: bool = False):
        self.order = FakeBrokerOrder()
        self.updates = 0
        self._update_raises = update_raises

    def update_order(self):
        self.updates += 1
        if self._update_raises:
            raise RuntimeError("update failed")


class FakeDataBroker:
    def __init__(self, answer):
        self.answer = answer
        self.calls = 0

    def confirm_order_is_finished_cancelling_if_required(self, order_with_controls):
        self.calls += 1
        if isinstance(self.answer, BaseException):
            raise self.answer

        return self.answer


class FakeContractStack:
    def __init__(self):
        self.released = []

    def release_order_from_algo_control(self, order_id):
        self.released.append(order_id)


class BrokerOrderHandlerUnderTest(stackHandlerCreateBrokerOrders):
    # No dataBlob, no database: only the collaborators post_trade_processing uses
    def __init__(self, data_broker):
        self._log = FakeLog()
        self._contract_stack = FakeContractStack()
        self._fake_data_broker = data_broker
        self.trade_limit_calls = []
        self.fill_calls = []

    @property
    def data_broker(self):
        return self._fake_data_broker

    def add_trade_to_trade_limits(self, executed_order):
        self.trade_limit_calls.append(executed_order)

    def apply_broker_order_fills_to_database(self, broker_order_id, broker_order):
        self.fill_calls.append(broker_order_id)


def test_contract_order_released_when_broker_order_finished():
    handler = BrokerOrderHandlerUnderTest(FakeDataBroker(True))
    order_with_controls = FakeOrderWithControls()

    handler.post_trade_processing(order_with_controls)

    assert handler.contract_stack.released == [4966]
    assert handler.log.messages("critical") == []
    assert handler.data_broker.calls == 1
    assert order_with_controls.updates == 1
    assert len(handler.trade_limit_calls) == 1
    assert handler.fill_calls == [6811]


def test_contract_order_not_released_when_broker_order_not_confirmed_finished():
    handler = BrokerOrderHandlerUnderTest(FakeDataBroker(False))

    handler.post_trade_processing(FakeOrderWithControls())

    assert handler.contract_stack.released == []
    assert len(handler.log.messages("critical")) == 1
    # trade limits and fills are still recorded
    assert len(handler.trade_limit_calls) == 1
    assert handler.fill_calls == [6811]


def test_exception_whilst_confirming_means_not_released_and_does_not_raise():
    handler = BrokerOrderHandlerUnderTest(FakeDataBroker(RuntimeError("boom")))

    handler.post_trade_processing(FakeOrderWithControls())

    assert handler.contract_stack.released == []
    assert len(handler.log.messages("critical")) == 1
    assert handler.fill_calls == [6811]


def test_failure_to_refresh_order_does_not_raise_or_block_release():
    handler = BrokerOrderHandlerUnderTest(FakeDataBroker(True))

    handler.post_trade_processing(FakeOrderWithControls(update_raises=True))

    assert handler.contract_stack.released == [4966]
    assert handler.log.messages("critical") == []


def test_overfilled_contract_order_is_not_released_even_if_broker_order_finished():
    handler = BrokerOrderHandlerUnderTest(FakeDataBroker(True))
    # as the fills sweep would have flagged it
    handler._overfilled_contract_order_ids.add(4966)

    handler.post_trade_processing(FakeOrderWithControls())

    assert handler.contract_stack.released == []
    # the overfill itself has already been reported once, elsewhere
    assert handler.log.messages("critical") == []


# ---------------------------------------------------------------------------
# Stack handler: an overfill must not kill the fills sweep
# ---------------------------------------------------------------------------


class FakeLocks:
    locked = []

    def __init__(self, data):
        pass

    def is_instrument_locked(self, instrument_code):
        return instrument_code in FakeLocks.locked

    def add_lock_for_instrument(self, instrument_code):
        FakeLocks.locked.append(instrument_code)


class FakeContractOrder:
    order_id = 4966
    instrument_code = "US10"
    children = [6811, 6812, 6813]
    trade = "[1, -1]"
    fill = "[1, -1]"

    def log_with_attributes(self, log):
        return log

    def __repr__(self):
        return "FakeContractOrder(4966)"


class FakeBrokerOrderList:
    def all_zero_fills(self):
        return False

    def final_fill_datetime(self):
        return NOW

    def total_filled_qty(self):
        return "[2, -2]"

    def average_fill_price(self):
        return 0.171875


class FakeStack:
    def __init__(self, order=None, order_list=None):
        self._order = order
        self._order_list = order_list
        self.algo_refs_added = []

    def get_order_with_id_from_stack(self, order_id):
        return self._order

    def get_list_of_orders_from_order_id_list(self, list_of_order_ids):
        return self._order_list

    def add_controlling_algo_ref(self, order_id, control_algo_ref):
        self.algo_refs_added.append((order_id, control_algo_ref))


class FillsHandlerUnderTest(stackHandlerForFills):
    def __init__(self, raise_overfill: bool):
        self._log = FakeLog()
        self._data = None
        self._contract_stack = FakeStack(order=FakeContractOrder())
        self._broker_stack = FakeStack(order_list=FakeBrokerOrderList())
        self._raise_overfill = raise_overfill
        self.apply_calls = 0

    def apply_fills_to_contract_order(self, **kwargs):
        self.apply_calls += 1
        if self._raise_overfill:
            raise overFilledOrder(
                "Can't fill order with fill [2, -2] more than trade quantity [1, -1]"
            )


@pytest.fixture
def fake_locks(monkeypatch):
    FakeLocks.locked = []
    monkeypatch.setattr(fills_module, "dataLocks", FakeLocks)

    return FakeLocks


def test_overfill_does_not_raise_locks_instrument_and_is_reported_once(fake_locks):
    handler = FillsHandlerUnderTest(raise_overfill=True)

    # the sweep runs continuously
    for _ in range(3):
        handler.apply_broker_fills_to_contract_order(4966)

    assert handler.apply_calls == 3
    assert fake_locks.locked == ["US10"]
    assert len(handler.log.messages("critical")) == 1
    # and no more broker orders can be created for this contract order
    assert handler.contract_stack.algo_refs_added == [
        (4966, fills_module.OVERFILLED_CONTROLLING_ALGO_REF)
    ]
    assert handler.is_contract_order_flagged_as_overfilled(4966)
    assert not handler.is_contract_order_flagged_as_overfilled(4967)


def test_overfill_on_already_locked_instrument_does_not_lock_again(fake_locks):
    fake_locks.locked = ["US10"]
    handler = FillsHandlerUnderTest(raise_overfill=True)

    handler.apply_broker_fills_to_contract_order(4966)

    assert fake_locks.locked == ["US10"]
    assert len(handler.log.messages("critical")) == 1


def test_normal_fill_is_applied_with_no_lock_and_no_alert(fake_locks):
    handler = FillsHandlerUnderTest(raise_overfill=False)

    handler.apply_broker_fills_to_contract_order(4966)

    assert handler.apply_calls == 1
    assert fake_locks.locked == []
    assert handler.log.records == []
