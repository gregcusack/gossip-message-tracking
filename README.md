# gossip-message-tracking

Create a .env file with your database settings in the following format:
```
db=<db-name>
u=<username>
p=<password>
port=<port>
host=<hostname>
```

Example:
```
db=development-database
u=greg
p=*********************
port=8086
host=internal-metrics.solana.com
```


### Fraction Duplicate Push
```
python main.py dup-push <optional: top_n_validators_by_stake>
```

Will plot fraction of duplicate push messages seen over last 14 days
```
num_duplicate_push / (all_push_success + all_push_fail)
```