import json
from decimal import Decimal
from collections import deque
from websockets.sync.client import connect
from websockets.client import connect as connect_async
from lighter.configuration import Configuration
from lighter.signer_client import SignerClient
from lighter.api_client import ApiClient
from lighter.api.transaction_api import TransactionApi


class _WsClientBase:
    """Base class for WebSocket clients with shared logic."""

    def __init__(
        self,
        host=None,
        path="/stream",
        # Transaction signing parameters
        private_key=None,
        account_index=None,
        api_key_index=None,
        # Logger
        logger=None,
        # Callbacks
        on_connected=None,
        on_tx_response=None,
        on_tx_batch_response=None,
        on_order_book_update=None,
        on_market_stats_update=None,
        on_trade_update=None,
        on_account_all_update=None,
        on_account_market_update=None,
        on_account_stats_update=None,
        on_transaction_update=None,
        on_executed_transaction_update=None,
        on_account_tx_update=None,
        on_account_all_orders_update=None,
        on_height_update=None,
        on_pool_data_update=None,
        on_pool_info_update=None,
        on_notification_update=None,
        on_account_orders_update=None,
        on_account_all_trades_update=None,
        on_account_all_positions_update=None,
    ):
        if host is None:
            host = Configuration.get_default().host.replace("https://", "")

        self.host = host
        self.base_url = f"wss://{host}{path}"
        self.http_url = f"https://{host}"

        # Logger setup
        self.logger = logger

        # Transaction signing setup
        self.private_key = private_key
        self.account_index = account_index
        self.api_key_index = api_key_index
        self.signer_client: SignerClient | None = None
        self.api_client: ApiClient | None = None
        self.transaction_api: TransactionApi | None = None
        self.auth_token: str | None = None

        # State management for different channels (ordered by API documentation)
        self.order_book = {}              # 1. Order Book
        self.market_stats = {}            # 2. Market Stats
        self.trade = {}                   # 3. Trade
        self.account_all = {}             # 4. Account All
        self.account_market = {}          # 5. Account Market
        self.account_stats = {}           # 6. Account Stats
        # 7. Transaction (no state)
        # 8. Executed Transaction (no state)
        # 9. Account Tx (no state)
        self.account_all_orders = {}      # 10. Account All Orders
        self.current_height = None        # 11. Height
        self.pool_data = {}               # 12. Pool Data
        self.pool_info = {}               # 13. Pool Info
        self.notification = {}            # 14. Notification
        self.account_orders = {}          # 15. Account Orders
        self.account_all_trades = {}      # 16. Account All Trades
        self.account_all_positions = {}   # 17. Account All Positions

        # Callback functions (ordered by API documentation)
        self.on_connected = on_connected
        self.on_tx_response = on_tx_response
        self.on_tx_batch_response = on_tx_batch_response
        self.on_order_book_update = on_order_book_update              # 1. Order Book
        self.on_market_stats_update = on_market_stats_update          # 2. Market Stats
        self.on_trade_update = on_trade_update                        # 3. Trade
        self.on_account_all_update = on_account_all_update            # 4. Account All
        self.on_account_market_update = on_account_market_update      # 5. Account Market
        self.on_account_stats_update = on_account_stats_update        # 6. Account Stats
        self.on_transaction_update = on_transaction_update            # 7. Transaction
        self.on_executed_transaction_update = on_executed_transaction_update  # 8. Executed Transaction
        self.on_account_tx_update = on_account_tx_update              # 9. Account Tx
        self.on_account_all_orders_update = on_account_all_orders_update  # 10. Account All Orders
        self.on_height_update = on_height_update                      # 11. Height
        self.on_pool_data_update = on_pool_data_update                # 12. Pool Data
        self.on_pool_info_update = on_pool_info_update                # 13. Pool Info
        self.on_notification_update = on_notification_update          # 14. Notification
        self.on_account_orders_update = on_account_orders_update      # 15. Account Orders
        self.on_account_all_trades_update = on_account_all_trades_update    # 16. Account All Trades
        self.on_account_all_positions_update = on_account_all_positions_update  # 17. Account All Positions

        self.ws = None
        self._running = False
        self.subscription = []
        self.nonce = None

    def _process_message(self, message):
        """Process a message (shared logic between sync and async)."""
        if isinstance(message, str):
            message = json.loads(message)

        # Handle error messages first
        error_info = message.get('error')
        if error_info:
            self.handle_error_message(error_info)
            return

        message_type = message.get("type")

        if message_type == "connected":
            return ("connected", None)
        elif message_type == "jsonapi/sendtx":
            self.handle_tx_response(message)
        elif message_type == "jsonapi/sendtxbatch":
            self.handle_tx_batch_response(message)
        # 1. Order Book
        elif message_type == "subscribed/order_book":
            self.handle_subscribed_order_book(message)
        elif message_type == "update/order_book":
            self.handle_update_order_book(message)
        # 2. Market Stats
        elif message_type == "subscribed/market_stats":
            self.handle_subscribed_market_stats(message)
        elif message_type == "update/market_stats":
            self.handle_update_market_stats(message)
        # 3. Trade
        elif message_type == "subscribed/trade":
            self.handle_subscribed_trade(message)
        elif message_type == "update/trade":
            self.handle_update_trade(message)
        # 4. Account All
        elif message_type == "subscribed/account_all":
            self.handle_subscribed_account_all(message)
        elif message_type == "update/account_all":
            self.handle_update_account_all(message)
        # 5. Account Market
        elif message_type == "subscribed/account_market":
            self.handle_subscribed_account_market(message)
        elif message_type == "update/account_market":
            self.handle_update_account_market(message)
        # 6. Account Stats
        elif message_type == "subscribed/user_stats":
            self.handle_subscribed_account_stats(message)
        elif message_type == "update/user_stats":
            self.handle_update_account_stats(message)
        # 7. Transaction
        elif message_type == "subscribed/transaction":
            self.handle_subscribed_transaction(message)
        elif message_type == "update/transaction":
            self.handle_update_transaction(message)
        # 8. Executed Transaction
        elif message_type == "subscribed/executed_transaction":
            self.handle_subscribed_executed_transaction(message)
        elif message_type == "update/executed_transaction":
            self.handle_update_executed_transaction(message)
        # 9. Account Tx
        elif message_type == "subscribed/account_tx":
            self.handle_subscribed_account_tx(message)
        elif message_type == "update/account_tx":
            self.handle_update_account_tx(message)
        # 10. Account All Orders
        elif message_type == "subscribed/account_all_orders":
            self.handle_subscribed_account_all_orders(message)
        elif message_type == "update/account_all_orders":
            self.handle_update_account_all_orders(message)
        # 11. Height
        elif message_type == "subscribed/height":
            self.handle_subscribed_height(message)
        elif message_type == "update/height":
            self.handle_update_height(message)
        # 12. Pool Data
        elif message_type == "subscribed/pool_data":
            self.handle_subscribed_pool_data(message)
        elif message_type == "update/pool_data":
            self.handle_update_pool_data(message)
        # 13. Pool Info
        elif message_type == "subscribed/pool_info":
            self.handle_subscribed_pool_info(message)
        elif message_type == "update/pool_info":
            self.handle_update_pool_info(message)
        # 14. Notification
        elif message_type == "subscribed/notification":
            self.handle_subscribed_notification(message)
        elif message_type == "update/notification":
            self.handle_update_notification(message)
        # 15. Account Orders
        elif message_type == "subscribed/account_orders":
            self.handle_subscribed_account_orders(message)
        elif message_type == "update/account_orders":
            self.handle_update_account_orders(message)
        # 16. Account All Trades
        elif message_type == "subscribed/account_all_trades":
            self.handle_subscribed_account_all_trades(message)
        elif message_type == "update/account_all_trades":
            self.handle_update_account_all_trades(message)
        # 17. Account All Positions
        elif message_type == "subscribed/account_all_positions":
            self.handle_subscribed_account_all_positions(message)
        elif message_type == "update/account_all_positions":
            self.handle_update_account_all_positions(message)
        # Ping/Pong
        elif message_type == "ping":
            return ("ping", None)
        else:
            self.handle_unhandled_message(message)

        return (None, None)

    def _record_subscription(self, method_name, args, kwargs):
        """Store subscription requests for later replay."""
        args_tuple = tuple(args)
        kwargs_copy = dict(kwargs)
        kwargs_copy.pop("_replay", None)

        for entry in self.subscription:
            if (
                entry["method"] == method_name
                and entry["args"] == args_tuple
                and entry["kwargs"] == kwargs_copy
            ):
                return False

        self.subscription.append(
            {"method": method_name, "args": args_tuple, "kwargs": kwargs_copy}
        )
        return True

    def _replay_subscriptions_sync(self):
        for entry in self.subscription:
            method = getattr(self, entry["method"], None)
            if not method:
                continue
            kwargs = dict(entry["kwargs"])
            kwargs["_replay"] = True
            method(*entry["args"], **kwargs)

    async def _replay_subscriptions_async(self):
        for entry in self.subscription:
            method = getattr(self, entry["method"], None)
            if not method:
                continue
            kwargs = dict(entry["kwargs"])
            kwargs["_replay"] = True
            await method(*entry["args"], **kwargs)

    def _convert_numeric_strings_to_decimal(self, data):
        """
        Recursively convert numeric string values to Decimal.
        Handles dict, list, and primitive types.
        """
        if isinstance(data, dict):
            return {k: self._convert_numeric_strings_to_decimal(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self._convert_numeric_strings_to_decimal(item) for item in data]
        elif isinstance(data, str):
            # Try to convert string to Decimal if it's a valid number
            try:
                # Check if it looks like a number (contains only digits, dots, minus, plus)
                if data and (data[0].isdigit() or data[0] in '.-+') and any(c.isdigit() for c in data):
                    return Decimal(data)
            except:
                pass
        return data

    def handle_connected(self):
        if self.on_connected:
            self.on_connected(self)

    def handle_tx_response(self, message):
        tx_hash = message["tx_hash"]  # tx hash string
        if self.on_tx_response:
            self.on_tx_response(tx_hash)

    def handle_tx_batch_response(self, message):
        tx_hashes = message["tx_hash"]  # list of tx hashes
        if self.on_tx_batch_response:
            self.on_tx_batch_response(tx_hashes)

    # Handler functions (ordered by API documentation)

    # 1. Order Book handlers
    def handle_subscribed_order_book(self, message):
        market_id = message["channel"].split(":")[1]
        order_book = message["order_book"]

        # Ensure initial order book is sorted
        # asks: ascending (lowest price first), bids: descending (highest price first)
        order_book["asks"].sort(key=lambda x: Decimal(x["price"]))
        order_book["bids"].sort(key=lambda x: Decimal(x["price"]), reverse=True)

        self.order_book[market_id] = order_book
        if self.on_order_book_update:
            self.on_order_book_update(market_id, self.order_book[market_id])

    def handle_update_order_book(self, message):
        market_id = message["channel"].split(":")[1]
        self.update_order_book_state(market_id, message["order_book"])
        if self.on_order_book_update:
            self.on_order_book_update(market_id, self.order_book[market_id])

    def update_order_book_state(self, market_id, order_book):
        self.update_orders(
            order_book["asks"], self.order_book[market_id]["asks"], is_ask=True
        )
        self.update_orders(
            order_book["bids"], self.order_book[market_id]["bids"], is_ask=False
        )

    def update_orders(self, new_orders, existing_orders, is_ask=True):
        """
        Update order book orders and keep them sorted using optimized binary search.
        Assumes existing_orders is already sorted.
        Uses Decimal for precise financial calculations.

        Args:
            new_orders: New orders to apply
            existing_orders: Existing orders to update (modified in place, must be sorted)
            is_ask: True for asks (ascending price), False for bids (descending price)

        Time complexity: O(m * (log n + n)) where m = len(new_orders), n = len(existing_orders)
        - Binary search: O(log n) - no extra list creation
        - Insertion/deletion: O(n) in worst case (unavoidable with list data structure)
        """
        for new_order in new_orders:
            new_price = Decimal(new_order["price"])
            new_size = Decimal(new_order["size"])

            if not existing_orders:
                if new_size > 0:
                    existing_orders.append(new_order)
                continue

            # Binary search for the price position (optimized without creating extra lists)
            left, right = 0, len(existing_orders)
            while left < right:
                mid = (left + right) // 2
                mid_price = Decimal(existing_orders[mid]["price"])

                # asks: ascending (low to high), bids: descending (high to low)
                if is_ask:
                    if mid_price < new_price:
                        left = mid + 1
                    else:
                        right = mid
                else:
                    if mid_price > new_price:
                        left = mid + 1
                    else:
                        right = mid

            idx = left

            # Check if exact price exists at idx
            if idx < len(existing_orders) and Decimal(existing_orders[idx]["price"]) == new_price:
                # Price exists - update or remove
                if new_size == 0:
                    existing_orders.pop(idx)
                else:
                    existing_orders[idx]["size"] = new_order["size"]
            else:
                # Price doesn't exist - insert if size > 0
                if new_size > 0:
                    existing_orders.insert(idx, new_order)

    # 2. Market Stats handlers
    def handle_subscribed_market_stats(self, message):
        self.handle_update_market_stats(message)

    def handle_update_market_stats(self, message):
        market_id = message["channel"].split(":")[1]
        market_stats_data = message.get("market_stats", {})
        market_stats_data = self._convert_numeric_strings_to_decimal(market_stats_data)
        self.market_stats[market_id] = market_stats_data
        if self.on_market_stats_update:
            self.on_market_stats_update(market_id, market_stats_data)

    # 3. Trade handlers
    def handle_subscribed_trade(self, message):
        self.handle_update_trade(message)

    def handle_update_trade(self, message):
        market_id = message["channel"].split(":")[1]
        if market_id not in self.trade:
            self.trade[market_id] = deque()
        self.trade[market_id].extend(message.get("trades", []))
        if self.on_trade_update:
            self.on_trade_update(market_id, message.get("trades", []))

    # 4. Account All handlers
    def handle_subscribed_account_all(self, message):
        self.handle_update_account_all(message)

    def handle_update_account_all(self, message):
        account_id = message["channel"].split(":")[1]
        self.account_all[account_id] = message
        if self.on_account_all_update:
            self.on_account_all_update(account_id, self.account_all[account_id])

    # 5. Account Market handlers
    def handle_subscribed_account_market(self, message):
        self.handle_update_account_market(message)

    def handle_update_account_market(self, message):
        channel_parts = message["channel"].split(":")
        if len(channel_parts) >= 2:
            key = channel_parts[1]
            self.account_market[key] = message
            if self.on_account_market_update:
                self.on_account_market_update(key, message)

    # 6. Account Stats handlers
    def handle_subscribed_account_stats(self, message):
        self.handle_update_account_stats(message)

    def handle_update_account_stats(self, message):
        account_id = message["channel"].split(":")[1]
        stats = message.get("stats", {})
        # Convert numeric strings to Decimal
        stats = self._convert_numeric_strings_to_decimal(stats)
        self.account_stats[account_id] = stats
        if self.on_account_stats_update:
            self.on_account_stats_update(account_id, stats)

    # 7. Transaction handlers
    def handle_subscribed_transaction(self, message):
        self.handle_update_transaction(message)

    def handle_update_transaction(self, message):
        if self.on_transaction_update:
            self.on_transaction_update(message)

    # 8. Executed Transaction handlers
    def handle_subscribed_executed_transaction(self, message):
        self.handle_update_executed_transaction(message)

    def handle_update_executed_transaction(self, message):
        if self.on_executed_transaction_update:
            self.on_executed_transaction_update(message)

    # 9. Account Tx handlers
    def handle_subscribed_account_tx(self, message):
        self.handle_update_account_tx(message)

    def handle_update_account_tx(self, message):
        account_id = message["channel"].split(":")[1]
        if self.on_account_tx_update:
            self.on_account_tx_update(account_id, message)

    # 10. Account All Orders handlers
    def handle_subscribed_account_all_orders(self, message):
        self.handle_update_account_all_orders(message)

    def handle_update_account_all_orders(self, message):
        account_id = message["channel"].split(":")[1]
        orders_by_market = message.get("orders", {})

        # Initialize account_id dict if not exists
        if account_id not in self.account_all_orders:
            self.account_all_orders[account_id] = {}

        # Process orders for each market
        for market_id, orders in orders_by_market.items():
            # Initialize deque with maxlen=20 if not exists
            if market_id not in self.account_all_orders[account_id]:
                self.account_all_orders[account_id][market_id] = deque(maxlen=20)

            # Add new orders to the deque (FIFO - old orders automatically removed)
            for order in orders:
                self.account_all_orders[account_id][market_id].append(order)

        if self.on_account_all_orders_update:
            self.on_account_all_orders_update(account_id, self.account_all_orders[account_id])

    # 11. Height handlers
    def handle_subscribed_height(self, message):
        self.handle_update_height(message)

    def handle_update_height(self, message):
        self.current_height = message.get("height")
        if self.on_height_update:
            self.on_height_update(self.current_height)

    # 12. Pool Data handlers
    def handle_subscribed_pool_data(self, message):
        self.handle_update_pool_data(message)

    def handle_update_pool_data(self, message):
        account_id = message.get("account")
        if account_id:
            self.pool_data[str(account_id)] = message
            if self.on_pool_data_update:
                self.on_pool_data_update(account_id, message)

    # 13. Pool Info handlers
    def handle_subscribed_pool_info(self, message):
        self.handle_update_pool_info(message)

    def handle_update_pool_info(self, message):
        account_id = message["channel"].split(":")[1]
        self.pool_info[account_id] = message
        if self.on_pool_info_update:
            self.on_pool_info_update(account_id, message)

    # 14. Notification handlers
    def handle_subscribed_notification(self, message):
        self.handle_update_notification(message)

    def handle_update_notification(self, message):
        account_id = message["channel"].split(":")[1]
        self.notification[account_id] = message.get("notifs", [])
        if self.on_notification_update:
            self.on_notification_update(account_id, message.get("notifs", []))

    # 15. Account Orders handlers
    def handle_subscribed_account_orders(self, message):
        self.handle_update_account_orders(message)

    def handle_update_account_orders(self, message):
        channel_parts = message["channel"].split(":")
        if len(channel_parts) >= 2:
            market_id = channel_parts[1]
            account_id = message.get("account")
            key = f"{market_id}_{account_id}"
            self.account_orders[key] = message
            if self.on_account_orders_update:
                self.on_account_orders_update(market_id, account_id, message)

    # 16. Account All Trades handlers
    def handle_subscribed_account_all_trades(self, message):
        self.handle_update_account_all_trades(message)

    def handle_update_account_all_trades(self, message):
        account_id = message["channel"].split(":")[1]
        self.account_all_trades[account_id] = message
        if self.on_account_all_trades_update:
            self.on_account_all_trades_update(account_id, message)

    # 17. Account All Positions handlers
    def handle_subscribed_account_all_positions(self, message):
        self.handle_update_account_all_positions(message)

    def handle_update_account_all_positions(self, message):
        account_id = message["channel"].split(":")[1]
        positions = message.get("positions", {})
        for market_id, position in positions.items():
            position = self._convert_numeric_strings_to_decimal(position)
            self.account_all_positions[market_id] = position
        if self.on_account_all_positions_update:
            self.on_account_all_positions_update(account_id, positions)
    
    def handle_nonce_error(self):
        """Handle nonce error."""
        self.nonce = None
        if self.logger:
            self.logger.warning("Nonce error detected. Resetting nonce.")
        else:
            print("Nonce error detected. Resetting nonce.")

    def handle_error_message(self, error_info):
        """Handle error messages from the server."""
        error_code = error_info.get('code')
        error_message = error_info.get('message', '')

        # Log the error
        log_msg = f"Error message received: code={error_code}, message={error_message}"
        if self.logger:
            self.logger.error(log_msg)
        else:
            print(log_msg)

        # Add more error handlers here as needed
        # elif error_code == SOME_OTHER_CODE:
        #     self.handle_some_other_error(error_info)

        if error_code == 21104:
            self.handle_nonce_error()

    def handle_unhandled_message(self, message):
        """Handle unhandled messages."""
        error_msg = f"Unhandled message: {message}"
        if self.logger:
            self.logger.error(error_msg)
        else:
            print(error_msg)

    def create_auth_token(self):
        # Initialize SignerClient if credentials are provided
        if self.private_key is not None and self.account_index is not None and self.api_key_index is not None:
            self.signer_client = SignerClient(
                url=self.http_url,
                private_key=self.private_key,
                account_index=self.account_index,
                api_key_index=self.api_key_index,
            )
            self.api_client = self.signer_client.api_client
            self.transaction_api = self.signer_client.tx_api

            # Generate auth token automatically
            self.auth_token, _ = self.signer_client.create_auth_token_with_expiry()
        else:
            if self.logger:
                self.logger.warning("SignerClient not initialized. Provide private_key, account_index, and api_key_index.")
            else:
                print("SignerClient not initialized. Provide private_key, account_index, and api_key_index.")


class WsClient(_WsClientBase):
    """Synchronous WebSocket client for Lighter."""

    def _send_subscribe(self, channel_path, auth_token=None):
        """Send subscription message."""
        if not self.ws:
            raise Exception("WebSocket is not connected.")

        msg = {"type": "subscribe", "channel": channel_path}
        token = auth_token or self.auth_token
        if token:
            msg["auth"] = token

        self.ws.send(json.dumps(msg))

    # Subscribe methods (ordered by API documentation)

    # 1. Order Book
    def subscribe_order_book(self, market_id, _replay=False):
        """Subscribe to order book updates for a market."""
        if not _replay:
            self._record_subscription("subscribe_order_book", (market_id,), {})
        if self.ws:
            self._send_subscribe(f"order_book/{market_id}")

    # 2. Market Stats
    def subscribe_market_stats(self, market_id, _replay=False):
        """Subscribe to market stats updates for a market."""
        if not _replay:
            self._record_subscription("subscribe_market_stats", (market_id,), {})
        if self.ws:
            self._send_subscribe(f"market_stats/{market_id}")

    # 3. Trade
    def subscribe_trade(self, market_id, _replay=False):
        """Subscribe to trade updates for a market."""
        if not _replay:
            self._record_subscription("subscribe_trade", (market_id,), {})
        if self.ws:
            self._send_subscribe(f"trade/{market_id}")

    # 4. Account All
    def subscribe_account_all(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all account updates."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"account_all/{account_id}", auth_token)

    # 5. Account Market
    def subscribe_account_market(self, market_id, account_id, auth_token=None, _replay=False):
        """Subscribe to account market updates."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_market",
                (market_id, account_id),
                {"auth_token": auth_token},
            )
        if self.ws:
            self._send_subscribe(f"account_market/{market_id}/{account_id}", auth_token)

    # 6. Account Stats
    def subscribe_account_stats(self, account_id, auth_token=None, _replay=False):
        """Subscribe to account stats updates."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_stats", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"user_stats/{account_id}", auth_token)

    # 7. Transaction
    def subscribe_transaction(self, _replay=False):
        """Subscribe to all transactions."""
        if not _replay:
            self._record_subscription("subscribe_transaction", (), {})
        if self.ws:
            self._send_subscribe("transaction")

    # 8. Executed Transaction
    def subscribe_executed_transaction(self, _replay=False):
        """Subscribe to executed transactions."""
        if not _replay:
            self._record_subscription("subscribe_executed_transaction", (), {})
        if self.ws:
            self._send_subscribe("executed_transaction")

    # 9. Account Tx
    def subscribe_account_tx(self, account_id, auth_token=None, _replay=False):
        """Subscribe to account transactions."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_tx", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"account_tx/{account_id}", auth_token)

    # 10. Account All Orders
    def subscribe_account_all_orders(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all orders for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all_orders", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"account_all_orders/{account_id}", auth_token)

    # 11. Height
    def subscribe_height(self, _replay=False):
        """Subscribe to blockchain height updates."""
        if not _replay:
            self._record_subscription("subscribe_height", (), {})
        if self.ws:
            self._send_subscribe("height")

    # 12. Pool Data
    def subscribe_pool_data(self, account_id, auth_token=None, _replay=False):
        """Subscribe to pool data for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_pool_data", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"pool_data/{account_id}", auth_token)

    # 13. Pool Info
    def subscribe_pool_info(self, account_id, auth_token=None, _replay=False):
        """Subscribe to pool info for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_pool_info", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"pool_info/{account_id}", auth_token)

    # 14. Notification
    def subscribe_notification(self, account_id, auth_token=None, _replay=False):
        """Subscribe to notifications for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_notification", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"notification/{account_id}", auth_token)

    # 15. Account Orders
    def subscribe_account_orders(self, market_id, account_id, auth_token=None, _replay=False):
        """Subscribe to account orders for a specific market."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_orders",
                (market_id, account_id),
                {"auth_token": auth_token},
            )
        if self.ws:
            self._send_subscribe(f"account_orders/{market_id}/{account_id}", auth_token)

    # 16. Account All Trades
    def subscribe_account_all_trades(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all trades for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all_trades", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"account_all_trades/{account_id}", auth_token)

    # 17. Account All Positions
    def subscribe_account_all_positions(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all positions for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all_positions", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            self._send_subscribe(f"account_all_positions/{account_id}", auth_token)

    def connect(self):
        self.create_auth_token()
        """Connect to WebSocket server."""
        self.ws = connect(self.base_url)
        self._running = True

        # Receive and process the first 'connected' message
        first_message = next(iter(self.ws))
        msg_type, _ = self._process_message(first_message)
        if msg_type == "connected":
            self.handle_connected()
            if self.subscription:
                self._replay_subscriptions_sync()

    def run(self):
        """
        Run the WebSocket message loop (blocking call).
        WebSocket must be connected first via connect().
        """
        if not self.ws:
            self.connect()

        try:
            for message in self.ws:
                if not self._running:
                    break
                msg_type, _ = self._process_message(message)
                if msg_type == "ping":
                    self.ws.send(json.dumps({"type": "pong"}))
        finally:
            self._running = False
            if self.ws:
                self.close()

    def stop(self):
        """Stop the WebSocket message loop."""
        self._running = False

    def close(self):
        """Close WebSocket and API client connections."""
        self.stop()
        if self.ws:
            self.ws.close()
            self.ws = None
        if self.signer_client:
            self.signer_client.close()
            self.signer_client = None
            self.api_client = None

    def __enter__(self):
        """Context manager entry."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False


class WsClientAsync(_WsClientBase):
    """Asynchronous WebSocket client for Lighter."""

    async def _send_subscribe(self, channel_path, auth_token=None):
        """Send subscription message (async)."""
        if not self.ws:
            raise Exception("WebSocket is not connected.")

        msg = {"type": "subscribe", "channel": channel_path}
        token = auth_token or self.auth_token
        if token:
            msg["auth"] = token

        await self.ws.send(json.dumps(msg))

    # Subscribe methods (ordered by API documentation)

    # 1. Order Book
    async def subscribe_order_book(self, market_id, _replay=False):
        """Subscribe to order book updates for a market."""
        if not _replay:
            self._record_subscription("subscribe_order_book", (market_id,), {})
        if self.ws:
            await self._send_subscribe(f"order_book/{market_id}")

    # 2. Market Stats
    async def subscribe_market_stats(self, market_id, _replay=False):
        """Subscribe to market stats updates for a market."""
        if not _replay:
            self._record_subscription("subscribe_market_stats", (market_id,), {})
        if self.ws:
            await self._send_subscribe(f"market_stats/{market_id}")

    # 3. Trade
    async def subscribe_trade(self, market_id, _replay=False):
        """Subscribe to trade updates for a market."""
        if not _replay:
            self._record_subscription("subscribe_trade", (market_id,), {})
        if self.ws:
            await self._send_subscribe(f"trade/{market_id}")

    # 4. Account All
    async def subscribe_account_all(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all account updates."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"account_all/{account_id}", auth_token)

    # 5. Account Market
    async def subscribe_account_market(
        self, market_id, account_id, auth_token=None, _replay=False
    ):
        """Subscribe to account market updates."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_market",
                (market_id, account_id),
                {"auth_token": auth_token},
            )
        if self.ws:
            await self._send_subscribe(f"account_market/{market_id}/{account_id}", auth_token)

    # 6. Account Stats
    async def subscribe_account_stats(self, account_id, auth_token=None, _replay=False):
        """Subscribe to account stats updates."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_stats", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"user_stats/{account_id}", auth_token)

    # 7. Transaction
    async def subscribe_transaction(self, _replay=False):
        """Subscribe to all transactions."""
        if not _replay:
            self._record_subscription("subscribe_transaction", (), {})
        if self.ws:
            await self._send_subscribe("transaction")

    # 8. Executed Transaction
    async def subscribe_executed_transaction(self, _replay=False):
        """Subscribe to executed transactions."""
        if not _replay:
            self._record_subscription("subscribe_executed_transaction", (), {})
        if self.ws:
            await self._send_subscribe("executed_transaction")

    # 9. Account Tx
    async def subscribe_account_tx(self, account_id, auth_token=None, _replay=False):
        """Subscribe to account transactions."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_tx", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"account_tx/{account_id}", auth_token)

    # 10. Account All Orders
    async def subscribe_account_all_orders(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all orders for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all_orders", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"account_all_orders/{account_id}", auth_token)

    # 11. Height
    async def subscribe_height(self, _replay=False):
        """Subscribe to blockchain height updates."""
        if not _replay:
            self._record_subscription("subscribe_height", (), {})
        if self.ws:
            await self._send_subscribe("height")

    # 12. Pool Data
    async def subscribe_pool_data(self, account_id, auth_token=None, _replay=False):
        """Subscribe to pool data for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_pool_data", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"pool_data/{account_id}", auth_token)

    # 13. Pool Info
    async def subscribe_pool_info(self, account_id, auth_token=None, _replay=False):
        """Subscribe to pool info for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_pool_info", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"pool_info/{account_id}", auth_token)

    # 14. Notification
    async def subscribe_notification(self, account_id, auth_token=None, _replay=False):
        """Subscribe to notifications for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_notification", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"notification/{account_id}", auth_token)

    # 15. Account Orders
    async def subscribe_account_orders(
        self, market_id, account_id, auth_token=None, _replay=False
    ):
        """Subscribe to account orders for a specific market."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_orders",
                (market_id, account_id),
                {"auth_token": auth_token},
            )
        if self.ws:
            await self._send_subscribe(f"account_orders/{market_id}/{account_id}", auth_token)

    # 16. Account All Trades
    async def subscribe_account_all_trades(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all trades for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all_trades", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"account_all_trades/{account_id}", auth_token)

    # 17. Account All Positions
    async def subscribe_account_all_positions(self, account_id, auth_token=None, _replay=False):
        """Subscribe to all positions for an account."""
        if not _replay:
            self._record_subscription(
                "subscribe_account_all_positions", (account_id,), {"auth_token": auth_token}
            )
        if self.ws:
            await self._send_subscribe(f"account_all_positions/{account_id}", auth_token)

    async def connect(self):
        self.create_auth_token()
        """Connect to WebSocket server (async)."""
        self.ws = await connect_async(self.base_url)
        self._running = True

        # Receive and process the first 'connected' message
        first_message = await self.ws.recv()
        msg_type, _ = self._process_message(first_message)
        if msg_type == "connected":
            self.handle_connected()
            if self.subscription:
                await self._replay_subscriptions_async()

    async def run(self):
        """
        Run the WebSocket message loop (async).
        WebSocket must be connected first via connect().
        """
        if not self.ws:
            await self.connect()
        while self._running:
            try:
                async for message in self.ws:
                    if not self._running:
                        break
                    msg_type, _ = self._process_message(message)
                    if msg_type == "ping":
                        await self.ws.send(json.dumps({"type": "pong"}))
            except Exception as e:
                if self.logger:
                    self.logger.exception("Exception in WS client: %s\n" % e)
                else:
                    print("Exception in WS client: %s\n" % e)
                if self.ws:
                    await self.close()
                await self.connect()
        if self.ws:
            await self.close()

    def stop(self):
        """Stop the WebSocket message loop."""
        self._running = False

    async def get_nonce(self):
        """Get the next nonce for the account."""
        next_nonce = await self.transaction_api.next_nonce(
            account_index=self.account_index,
            api_key_index=self.api_key_index
        )
        self.nonce = next_nonce.nonce

    async def create_order(
        self,
        market_index,
        client_order_index,
        base_amount,
        price,
        is_ask,
        order_type,
        time_in_force=None,
        order_expiry=None,
        reduce_only=False,
        trigger_price=0,
        request_id=None,
    ):
        """
        Create and send an order via WebSocket.

        Args:
            market_index: Market index
            client_order_index: Client order index (unique identifier)
            base_amount: Base amount (scaled by 1e10)
            price: Price (scaled by 1e2)
            is_ask: True for sell, False for buy
            order_type: Order type (e.g., SignerClient.ORDER_TYPE_LIMIT)
            time_in_force: Time in force (optional)
            order_expiry: Order expiry (optional)
            reduce_only: Whether order is reduce-only
            trigger_price: Trigger price for stop orders
            request_id: Optional request ID to track the response

        Returns:
            response: WebSocket response message
        """
        if not self.ws:
            if self.logger:
                self.logger.error("WebSocket is not connected. Call connect() first.")
            else:
                print("WebSocket is not connected. Call connect() first.")
            await self.connect()

        if not self.signer_client:
            if self.logger:
                self.logger.error("Initializing SignerClient...")
            else:
                print("Initializing SignerClient...")
            self.create_auth_token()

        if self.nonce is None:
            await self.get_nonce()

        # Sign the order
        # Set defaults if not provided
        if time_in_force is None:
            time_in_force = SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME

        if order_expiry is None:
            if time_in_force == SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL:
                order_expiry = SignerClient.DEFAULT_IOC_EXPIRY
            else:
                order_expiry = SignerClient.DEFAULT_28_DAY_ORDER_EXPIRY

        tx_info, error = self.signer_client.sign_create_order(
            market_index=market_index,
            client_order_index=client_order_index,
            base_amount=base_amount,
            price=price,
            is_ask=is_ask,
            order_type=order_type,
            time_in_force=time_in_force,
            order_expiry=order_expiry,
            reduce_only=reduce_only,
            trigger_price=trigger_price,
            nonce=self.nonce,
        )
        self.nonce += 1

        if error is not None:
            raise Exception(f"Error signing order: {error}")

        # Send via WebSocket
        msg = {
            "type": "jsonapi/sendtx",
            "data": {
                "tx_type": SignerClient.TX_TYPE_CREATE_ORDER,
                "tx_info": json.loads(tx_info),
            }
        }

        if request_id:
            msg["data"]["id"] = request_id

        await self.ws.send(json.dumps(msg))

    async def create_limit_order(
        self,
        market_index,
        client_order_index,
        base_amount,
        price,
        is_ask,
        reduce_only=False,
        time_in_force=None,
        order_expiry=None,
        request_id=None,
    ):
        """
        Create a limit order via WebSocket.

        Args:
            market_index: Market index
            client_order_index: Client order index (unique identifier)
            base_amount: Base amount (scaled by 1e10)
            price: Limit price (scaled by 1e2)
            is_ask: True for sell, False for buy
            reduce_only: Whether order is reduce-only
            time_in_force: Time in force (optional)
            order_expiry: Order expiry (optional)
            request_id: Optional request ID to track the response

        Returns:
            response: WebSocket response message
        """
        return await self.create_order(
            market_index=market_index,
            client_order_index=client_order_index,
            base_amount=base_amount,
            price=price,
            is_ask=is_ask,
            order_type=SignerClient.ORDER_TYPE_LIMIT,
            time_in_force=time_in_force,
            order_expiry=order_expiry,
            reduce_only=reduce_only,
            trigger_price=0,
            request_id=request_id,
        )

    async def create_market_order(
        self,
        market_index,
        client_order_index,
        base_amount,
        avg_execution_price,
        is_ask,
        reduce_only=False,
        request_id=None,
    ):
        """
        Create a market order via WebSocket.

        Args:
            market_index: Market index
            client_order_index: Client order index (unique identifier)
            base_amount: Base amount (scaled by 1e10)
            avg_execution_price: Worst acceptable execution price (scaled by 1e2)
            is_ask: True for sell, False for buy
            reduce_only: Whether order is reduce-only
            request_id: Optional request ID to track the response

        Returns:
            response: WebSocket response message
        """
        return await self.create_order(
            market_index=market_index,
            client_order_index=client_order_index,
            base_amount=base_amount,
            price=avg_execution_price,
            is_ask=is_ask,
            order_type=SignerClient.ORDER_TYPE_MARKET,
            time_in_force=SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
            order_expiry=SignerClient.DEFAULT_IOC_EXPIRY,
            reduce_only=reduce_only,
            trigger_price=0,
            request_id=request_id,
        )

    async def create_order_batch(
        self,
        orders,
        request_id=None,
    ):
        """
        Create and send multiple orders in a single batch via WebSocket.

        Args:
            orders: List of order dictionaries, each containing:
                - market_index: Market index
                - client_order_index: Client order index (unique identifier)
                - base_amount: Base amount (scaled by 1e10)
                - price: Price (scaled by 1e2)
                - is_ask: True for sell, False for buy
                - order_type: Order type (e.g., SignerClient.ORDER_TYPE_LIMIT)
                - time_in_force: Time in force (optional)
                - order_expiry: Order expiry (optional)
                - reduce_only: Whether order is reduce-only (optional, default False)
                - trigger_price: Trigger price for stop orders (optional, default 0)
            request_id: Optional request ID to track the response

        Returns:
            None (response will be handled by on_tx_batch_response callback)

        Example:
            orders = [
                {
                    "market_index": 48,
                    "client_order_index": 1,
                    "base_amount": 10000000,
                    "price": 407119,
                    "is_ask": True,
                    "order_type": SignerClient.ORDER_TYPE_LIMIT,
                },
                {
                    "market_index": 92,
                    "client_order_index": 2,
                    "base_amount": 5000000,
                    "price": 270050,
                    "is_ask": False,
                    "order_type": SignerClient.ORDER_TYPE_LIMIT,
                }
            ]
            await ws_client.create_order_batch(orders)
        """
        if not self.ws:
            if self.logger:
                self.logger.error("WebSocket is not connected. Call connect() first.")
            else:
                print("WebSocket is not connected. Call connect() first.")
            await self.connect()

        if not self.signer_client:
            if self.logger:
                self.logger.error("SignerClient not initialized. Provide private_key, account_index, and api_key_index.")
            else:
                print("SignerClient not initialized. Provide private_key, account_index, and api_key_index.")
            self.create_auth_token()

        if not orders:
            raise Exception("No orders provided.")

        tx_types = []
        tx_infos = []
        if self.nonce is None:
            await self.get_nonce()
        for order in orders:

            # Set defaults if not provided
            time_in_force = order.get("time_in_force")
            if time_in_force is None:
                time_in_force = SignerClient.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME

            order_expiry = order.get("order_expiry")
            if order_expiry is None:
                if time_in_force == SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL:
                    order_expiry = SignerClient.DEFAULT_IOC_EXPIRY
                else:
                    order_expiry = SignerClient.DEFAULT_28_DAY_ORDER_EXPIRY

            # Sign the order
            tx_info, error = self.signer_client.sign_create_order(
                market_index=order["market_index"],
                client_order_index=order["client_order_index"],
                base_amount=order["base_amount"],
                price=order["price"],
                is_ask=order["is_ask"],
                order_type=order["order_type"],
                time_in_force=time_in_force,
                order_expiry=order_expiry,
                reduce_only=order.get("reduce_only", False),
                trigger_price=order.get("trigger_price", 0),
                nonce=self.nonce,
            )
            self.nonce += 1

            if error is not None:
                raise Exception(f"Error signing order {len(tx_infos)}: {error}")

            tx_types.append(SignerClient.TX_TYPE_CREATE_ORDER)
            tx_infos.append(tx_info)

        # Send batch via WebSocket
        msg = {
            "type": "jsonapi/sendtxbatch",
            "data": {
                "tx_types": json.dumps(tx_types),
                "tx_infos": json.dumps(tx_infos),
            }
        }

        if request_id:
            msg["data"]["id"] = request_id

        await self.ws.send(json.dumps(msg))

    async def create_limit_order_batch(
        self,
        orders,
        request_id=None,
    ):
        """
        Create multiple limit orders in a single batch via WebSocket.

        Args:
            orders: List of order dictionaries, each containing:
                - market_index: Market index
                - client_order_index: Client order index (unique identifier)
                - base_amount: Base amount (scaled by 1e10)
                - price: Limit price (scaled by 1e2)
                - is_ask: True for sell, False for buy
                - reduce_only: Whether order is reduce-only (optional, default False)
                - time_in_force: Time in force (optional)
                - order_expiry: Order expiry (optional)
            request_id: Optional request ID to track the response

        Returns:
            None (response will be handled by on_tx_batch_response callback)
        """
        # Add order_type to each order
        orders_with_type = [
            {
                **order,
                "order_type": SignerClient.ORDER_TYPE_LIMIT,
            }
            for order in orders
        ]

        await self.create_order_batch(orders_with_type, request_id)

    async def create_market_order_batch(
        self,
        orders,
        request_id=None,
    ):
        """
        Create multiple market orders in a single batch via WebSocket.

        Args:
            orders: List of order dictionaries, each containing:
                - market_index: Market index
                - client_order_index: Client order index (unique identifier)
                - base_amount: Base amount (scaled by 1e10)
                - avg_execution_price: Worst acceptable execution price (scaled by 1e2)
                - is_ask: True for sell, False for buy
                - reduce_only: Whether order is reduce-only (optional, default False)
            request_id: Optional request ID to track the response

        Returns:
            None (response will be handled by on_tx_batch_response callback)
        """
        # Add order_type, time_in_force, and order_expiry to each order
        # Also rename avg_execution_price to price
        orders_with_type = [
            {
                "market_index": order["market_index"],
                "client_order_index": order["client_order_index"],
                "base_amount": order["base_amount"],
                "price": order["avg_execution_price"],
                "is_ask": order["is_ask"],
                "reduce_only": order.get("reduce_only", False),
                "order_type": SignerClient.ORDER_TYPE_MARKET,
                "time_in_force": SignerClient.ORDER_TIME_IN_FORCE_IMMEDIATE_OR_CANCEL,
                "order_expiry": SignerClient.DEFAULT_IOC_EXPIRY,
                "trigger_price": 0,
            }
            for order in orders
        ]

        await self.create_order_batch(orders_with_type, request_id)

    async def close(self):
        """Close WebSocket and API client connections (async)."""
        self.stop()
        if self.ws:
            await self.ws.close()
            self.ws = None
        if self.signer_client:
            await self.signer_client.close()
            self.signer_client = None
            self.api_client = None

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()
        return False
