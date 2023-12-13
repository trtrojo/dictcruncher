# DictCruncher - a library for flattening json objects

---

dictcruncher is a library for flattening objects into a format that can be easily imported into other libraries eg: pandas.DataFrame.from_records()


## Usage:

We can easily convert lists of json objects into flattened "rows" we can iterate over

```python
import dictcruncher as dc

in_list = [
    {
        "id": "000001337",
        "tripUpdate": {
            "trip": {
                "tripId": "1337",
                "startTime": "02:17:23",
                "startDate": "20231207",
                "routeId": "Blue"
            }
        }
    }
]

table_config = {
    'trip_config': [
        dm.MapperLocation(location='root::id', column_name='route_id'),
        dm.MapperLocation(location='root::tripUpdate::trip::tripId', column_name='trip_id'),
        dm.MapperLocation(location='root::tripUpdate::trip::startTime', column_name='start_time'),
        dm.MapperLocation(location='root::tripUpdate::trip::startDate', column_name='start_date'),
        dm.MapperLocation(location='root::tripUpdate::trip::routeId', column_name='routeId'),
    ]
}

in_dictcruncher = dm.dictcruncher(in_dict_list=in_list, mapper=table_config)

flattened = in_dictcruncher.get_records(table_name='trip_config')

# flattened
[
    {
        "route_id": "000001337",
        "trip_id": "1337",
        "start_time": "02:17:23",
        "start_date": "20231207",
        "routeId": "Blue"
    }
]
```

We can iterate over nested lists that are one level down (Currently it does not support more than one level.)
```python
import dictcruncher as dc

in_list = [
    {
        "id": "000001337",
        "timestamp": "1701938918",
        "[realtime_data]": {
            "tripRouteData": [
                {
                    "routeId": "Blue",
                    "nextTrain": {
                        "start": "1701940718"
                    }
                },
                {
                    "routeId": "Red",
                    "nextTrain": {
                        "start": "1701940718"
                    }
                },
                {
                    "routeId": "Orange",
                    "nextTrain": {
                        "start": "1701940718"
                    }
                }
            ]
        }
    }
]

table_config = {
    'routes': [
        dm.MapperLocation(location='root::id', column_name='route_id'),
        dm.MapperLocation(location='root::timestamp', column_name='route_timestamp'),
        dm.MapperLocation(location='root::[realtime_data]::tripRouteData[]::routeId', column_name='line_name'),
        dm.MapperLocation(location='root::[realtime_data]::tripRouteData[]::nextTrain::start', column_name='next_train_start'),
    ]
}

in_dictcruncher = dm.dictcruncher(in_dict_list=in_list, mapper=table_config)

flattened = in_dictcruncher.get_records(table_name='routes')

# flattened:
[
    {
        "route_id": "000001337",
        "route_timestamp": "1701938918",
        "line_name": "Blue",
        "next_train_start": "1701940718"
    },
    {
        "route_id": "000001337",
        "route_timestamp": "1701938918",
        "line_name": "Red",
        "next_train_start": "1701940718"
    },
    {
        "route_id": "000001337",
        "route_timestamp": "1701938918",
        "line_name": "Orange",
        "next_train_start": "1701940718"
    }
]
```

We also support passing in default values, and preforming operations on them

```python
import dictcruncher as dc

in_dict = [
    {
        "transaction_id": 1,
        "total_amount": 1000,
        "tip_amount": 300
    },
    {
        "transaction_id": 2,
        "total_amount": 1000
    },
    
]

table_config = {
    'transactions': [
        dm.MapperLocation(location='root::transaction_id', column_name='transaction_id'),
        dm.MapperLocation(location='root::total_amount', column_name='total_amount', coalesce_value=0),
        dm.MapperLocation(location='root::tip_amount', column_name='tip_amount', coalesce_value=0),
        dm.MapperLocation(location='root::tip_amount', column_name='tip_amount_negative', coalesce_value=0, convert_function=lambda x: x * -1)
    ]
}

in_dictcruncher = dm.dictcruncher(in_dict_list=in_dict, mapper=table_config)

flattened = in_dictcruncher.get_records(table_name='transactions')

# Flattened
[
    {
        "transaction_id": 1,
        "total_amount": 1000,
        "tip_amount": 300,
        "tip_amount_negative": -300
    },
    {
        "transaction_id": 2,
        "total_amount": 1000,
        "tip_amount": 0,
        "tip_amount_negative": 0
    }
]
```

Finally, we support error handling on incomplete objects
```python
import dictcruncher as dc

in_dict = [
    {
        "transaction_id": 1,
        "total_amount": 1000,
        "tip_amount": 300
    },
    {
        "transaction_id": 2,
        "total_amount": 1000
    },
    
]

table_config = {
    'tips_only': [
        dm.MapperLocation(location='root::transaction_id', column_name='transaction_id'),
        dm.MapperLocation(location='root::total_amount', column_name='total_amount', coalesce_value=0),
        
        # if_missing supports 'drop' to drop object or 'fail' to raise an exception.
        dm.MapperLocation(location='root::tip_amount', column_name='tip_amount', if_missing='drop')
    ]
}

in_dictcruncher = dm.dictcruncher(in_dict_list=in_dict, mapper=table_config)

flattened = in_dictcruncher.get_records(table_name='tips_only')

# Flattened
[
    {
        "transaction_id": 1,
        "total_amount": 1000,
        "tip_amount": 300
    }
]
```