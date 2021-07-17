import mysql.connector
from datetime import datetime
from locations import Locations
from constants import TAX_MULTIPLIER
import time
import math
import json


# Coalesce orders with the same price together by adding their quantities.
def coalesce_orders(order_list):

    orders_combined = list()
    for order in order_list:

        # If the list is empty, or the last order doesn't have the same price, append new order.
        if len(orders_combined) == 0 or orders_combined[-1]["price"] != order["price"]:
            orders_combined.append(order)
            continue

        # Otherwise list is not empty and last item has same price, so add the quantities.
        orders_combined[-1]["amount"] += order["amount"]

    return orders_combined


# Start
print("Started Script...")

# Connect to our MySql server and create a cursor
connect = mysql.connector.connect(user="root", password="rootrootroot", host="localhost", port="3306", database="albiondb")
cursor = connect.cursor(buffered=True, dictionary=True)

# Loop this script forever.
start_time = datetime.now()
while True:
    # For debug output
    print("Loop time: " + str(int((datetime.now() - start_time).total_seconds())))
    start_time = datetime.now()

    # Create the list in which we dump our profitable trade outputs
    output_list = []

    # Create our database caches.
    buy_order_cache  = {}
    sell_order_cache = {}

    # Get the list of unique item buy orders.
    cursor.execute("SELECT DISTINCT item_id, quality_level, location FROM albiondb.market_orders WHERE auction_type = 'request'")

    # Process each buy request.
    for item_requested in cursor.fetchall():

        # Create a more useful item ID that we'll use internally that also has the quality level.
        item_name_suffixed = str(item_requested["item_id"]) + "#" + str(item_requested["quality_level"])

        # Check if this item's buy order's are already in the cache.
        if item_name_suffixed not in buy_order_cache:

            # Grab all the buy orders for this item everywhere at once and cache them for performance reasons.
            cursor.execute(f"""SELECT location, price, amount FROM albiondb.market_orders 
                            WHERE item_id = "{item_requested["item_id"]}"
                            AND quality_level = {item_requested["quality_level"]} 
                            AND auction_type = 'request' 
                            ORDER BY location, price DESC""")
            all_location_buy_orders = cursor.fetchall()

            # Create the buy order cache entry for this item.
            buy_order_cache[item_name_suffixed] = {}

            # Populate the cache with each order.
            for order in all_location_buy_orders:

                # Create the location's list of buys's if they don't exist.
                if order["location"] not in buy_order_cache[item_name_suffixed]:
                    buy_order_cache[item_name_suffixed][order["location"]] = []

                # Add the order.
                buy_order_cache[item_name_suffixed][order["location"]].append({"price": order["price"], "amount": order["amount"]})

            # Now that we have the new orders in the cache, coalesce them.
            for loc in buy_order_cache[item_name_suffixed]:
                buy_order_cache[item_name_suffixed][loc] = coalesce_orders(buy_order_cache[item_name_suffixed][loc])

        # Now that we have our buy orders all grouped up nice and tidy,
        # loop through each other location and look for sell orders.
        for location in Locations:

            # Check if this item already had it's sell orders pulled for it.
            if item_name_suffixed not in sell_order_cache:

                # No sell orders exist in the cache for this item, so get them for all locations.
                cursor.execute(f"""SELECT location, price, amount FROM albiondb.market_orders 
                                WHERE item_id = "{item_requested["item_id"]}"
                                AND quality_level = {item_requested["quality_level"]} 
                                AND auction_type = 'offer' 
                                ORDER BY location, price ASC""")
                all_location_sell_orders = cursor.fetchall()

                # Create a dictionary in the sell order cache for this item.
                sell_order_cache[item_name_suffixed] = {}

                # Populate the cache with each order.
                for order in all_location_sell_orders:

                    # Create the location's list of sell's if they don't exist.
                    if order["location"] not in sell_order_cache[item_name_suffixed]:
                        sell_order_cache[item_name_suffixed][order["location"]] = []

                    # Add the order.
                    sell_order_cache[item_name_suffixed][order["location"]].append({"price": order["price"], "amount": order["amount"]})

                # Now that we have the new orders in the cache, coalesce them.
                for loc in sell_order_cache[item_name_suffixed]:
                    sell_order_cache[item_name_suffixed][loc] = coalesce_orders(sell_order_cache[item_name_suffixed][loc])

            # Skip this location if either:
            # The current location is the location of the buy order
            # The cache does not have any sell orders for this item at this location.
            if item_requested["location"] == location.value or location.value not in sell_order_cache[item_name_suffixed]:
                continue

            # Now pull the desired sell orders from the cache.
            # We create temporary holders because we're going to be
            # modifying these as we iterate through them.
            sell_orders_tmp = sell_order_cache[item_name_suffixed][location.value]
            buy_orders_tmp   = buy_order_cache[item_name_suffixed][item_requested["location"]]

            # Structures for our calculation loop.
            profit        = 0  # Profit aggregator.
            item_quantity = 0  # Quantity aggregator.

            sell_orders_to_fulfill = {}  # List of sell orders we're going to buy
            buy_orders_to_fulfill  = {}  # List of buy orders we're going to sell to.

            while len(buy_orders_tmp) != 0 and len(sell_orders_tmp) != 0 and (sell_orders_tmp[0]["price"] < (buy_orders_tmp[0]["price"] * TAX_MULTIPLIER)):

                # Increase the profit counter.
                # Tax always rounds up, so we round down the revenue from buy orders.
                profit += math.floor(buy_orders_tmp[0]["price"] * TAX_MULTIPLIER) - sell_orders_tmp[0]["price"]

                # Create entries if they don't exist already
                if sell_orders_tmp[0]["price"] not in sell_orders_to_fulfill:
                    sell_orders_to_fulfill[sell_orders_tmp[0]["price"]] = 0
                if buy_orders_tmp[0]["price"] not in buy_orders_to_fulfill:
                    buy_orders_to_fulfill[buy_orders_tmp[0]["price"]] = 0

                # Increment the orders we're going to buy and sell
                sell_orders_to_fulfill[sell_orders_tmp[0]["price"]] += 1
                buy_orders_to_fulfill[buy_orders_tmp[0]["price"]]  += 1

                # Decrease the amounts
                sell_orders_tmp[0]["amount"] -= 1
                buy_orders_tmp[0]["amount"] -= 1

                # Increase the quantity.
                item_quantity += 1

                # If the amount at that price is zero delete it
                # so we automatically move on to the next one.
                if sell_orders_tmp[0]["amount"] == 0:
                    del sell_orders_tmp[0]
                if buy_orders_tmp[0]["amount"] == 0:
                    del buy_orders_tmp[0]

            # If we can make money, output to CSV file.
            if profit != 0:

                # Try to convert the location ID to an actual location.
                # Use error string if we cannot.
                location_name = "ERROR_MISSING_LOCATION"
                try:
                    location_name = Locations(item_requested['location']).name
                except:
                    pass

                # Great, profit calculation is done. Output to shitty CSV file.
                output_list.append({
                    "item": str(item_requested["item_id"]) + "#" + str(item_requested["quality_level"]),
                    "from": location.name,
                    "to": location_name,
                    "quantity": item_quantity,
                    "profit": int(profit),  # Round
                    "sell_orders_to_fulfill": sell_orders_to_fulfill,
                    "buy_orders_to_fulfill": buy_orders_to_fulfill
                })

    # Write and close this file.
    outfile = open("PROFIT.json", "w")
    outfile.write(json.dumps(output_list))
    outfile.flush()
    outfile.close()
