## dbt overview

```mermaid
flowchart LR

subgraph Big Query
m[monthy data]
w[weekly data]
lu[fuel types lookup]
end

m --most_recent_only--> stations_snapshot --> ds
m --> mp
w --> p
lu --> dt

subgraph dbt
snapshot
staging
mart
end

subgraph staging
ds[dim stations]
dt[dim fuel types]
p[weekly prices]
mp[montly_prices]
end

p & mp --> up
dt --> up

subgraph snapshot
stations_snapshot
end

subgraph mart
up[table_wide_prices]
end

ds --> up
up --> cs
up --> as

subgraph activation_layer
cs[stations with outdated data]
as[display prices of active stations]
end
```

fuel type lookup as a dbt macro? would have to be hardcoded
