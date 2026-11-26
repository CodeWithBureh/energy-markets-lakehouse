# Data dictionary

Generated from Unity Catalog (`energy`) on 2026-11-26 by `scripts/data_dictionary.py`. Table descriptions are the comments set in `src/pipeline/`; units come from column-name suffixes, and descriptions of the less obvious columns from the script.

**Bronze** (`bronze.*`) holds the landed files as they arrived, append-only, with `_file_modified_at` and `_source_file` on every row: `elexon_demand_forecast`, `elexon_demand_forecast_daily`, `elexon_demand_outturn`, `elexon_generation_by_fuel`, `elexon_market_index`, `elexon_system_prices`, `elexon_wind_forecast`, `elexon_wind_solar_forecast`, `entsoe_day_ahead`, `era5_gb_hourly`, `fred_observations`, `gas_sap`, `gas_ttf`, `stock_daily_prices`, `weather_forecast_gb_hourly`, `weather_forecast_gb_hourly_archive`.

## Silver

Typed, deduplicated, UTC. Merged tables keep every revision (SCD type 2): current rows have `__END_AT IS NULL`, and `__START_AT` records the delivery that first brought each version.

| Table | Description |
|---|---|
| [`era5_gb_grid_hourly`](#silverera5_gb_grid_hourly) | ERA5 hourly weather per 0.25-degree grid cell over GB, in friendly units. (SCD type 2) |
| [`eu_day_ahead_prices`](#silvereu_day_ahead_prices) | Day-ahead prices (EUR/MWh) for GB's interconnected neighbours, native resolution. (SCD type 2) |
| [`fred_observations`](#silverfred_observations) | Latest value of every FRED observation (gas prices and FX), one row per day. (SCD type 2) |
| [`fx_rates_daily`](#silverfx_rates_daily) | Daily FX from FRED: USD per GBP, USD per EUR, and derived GBP per EUR. |
| [`gas_prices`](#silvergas_prices) | Natural gas benchmarks from FRED (USD/MMBtu). Henry Hub daily, EU monthly. |
| [`gb_demand_forecast`](#silvergb_demand_forecast) | GB national demand forecast (NDF, MW), every published vintage. (SCD type 2) |
| [`gb_demand_forecast_daily`](#silvergb_demand_forecast_daily) | GB national demand forecast 2-14 days ahead (NDFD): forecast daily PEAK demand (MW), every published vintage. (SCD type 2) |
| [`gb_demand_outturn`](#silvergb_demand_outturn) | GB demand outturn (MW) per settlement period: national (INDO) and transmission system (ITSDO). Both are net of embedded wind and solar. (SCD type 2) |
| [`gb_gas_sap_daily`](#silvergb_gas_sap_daily) | GB System Average Price (SAP) of gas per gas day (05:00-05:00 UK), in p/kWh and GBP/MWh. Source: National Gas Transmission. (SCD type 2) |
| [`gb_generation_by_fuel`](#silvergb_generation_by_fuel) | GB half-hourly generation outturn (MW) by fuel type, incl. interconnectors. (SCD type 2) |
| [`gb_market_index_prices`](#silvergb_market_index_prices) | GB market index price (GBP/MWh) and traded volume per settlement period and provider. APXMIDP carries the trades; N2EXMIDP reports zero volume. (SCD type 2) |
| [`gb_system_prices`](#silvergb_system_prices) | GB system (imbalance) price per settlement period, latest settlement run. (SCD type 2) |
| [`gb_wind_forecast`](#silvergb_wind_forecast) | Hourly wind generation forecast (WINDFOR, MW) for transmission-connected wind, every published vintage. (SCD type 2) |
| [`gb_wind_solar_forecast`](#silvergb_wind_solar_forecast) | Half-hourly solar, onshore and offshore wind forecasts (DGWS, MW): day-ahead and intraday, every published vintage. (SCD type 2) |
| [`stock_prices_daily`](#silverstock_prices_daily) | Daily share prices in GBP (London quotes converted from pence). (SCD type 2) |
| [`ttf_gas_daily`](#silverttf_gas_daily) | Dutch TTF front-month gas future, daily close (EUR/MWh), trading days only. (SCD type 2) |
| [`weather_forecast_gb_hourly`](#silverweather_forecast_gb_hourly) | Open-Meteo hourly weather forecasts at 1-degree points over GB, every daily fetch (issued_utc = fetch time). (SCD type 2) |
| [`weather_forecast_gb_hourly_archive`](#silverweather_forecast_gb_hourly_archive) | Past forecast issues rebuilt from Open-Meteo's Previous Runs API: what the 07:40 UK fetch would have returned each day. lead_days = how far ahead the forecast was made; lead_shortfall_hours > 0 where it was made after the issue time (late on D+7, as the API keeps 7 days of lead). (SCD type 2) |

### silver.era5_gb_grid_hourly

ERA5 hourly weather per 0.25-degree grid cell over GB, in friendly units.

| Column | Type | Unit | Description |
|---|---|---|---|
| `valid_time_utc` | timestamp | UTC |  |
| `latitude` | double |  |  |
| `longitude` | double |  |  |
| `temp_c` | double | deg C |  |
| `wind_speed_10m_ms` | double | m/s |  |
| `wind_speed_100m_ms` | double | m/s |  |
| `solar_irradiance_wm2` | double | W/m2 |  |

### silver.eu_day_ahead_prices

Day-ahead prices (EUR/MWh) for GB's interconnected neighbours, native resolution.

| Column | Type | Unit | Description |
|---|---|---|---|
| `zone` | string |  |  |
| `zone_eic` | string |  |  |
| `delivery_start_utc` | timestamp | UTC |  |
| `delivery_end_utc` | timestamp | UTC |  |
| `resolution_minutes` | int | minutes |  |
| `price_eur_mwh` | double | EUR/MWh |  |
| `is_forward_filled` | boolean |  | ENTSO-E A03 curve point filled from the previous position |
| `currency` | string |  |  |
| `curve_type` | string |  |  |
| `classification_sequence` | int |  | ENTSO-E sequence; 1 is the main auction |

### silver.fred_observations

Latest value of every FRED observation (gas prices and FX), one row per day.

| Column | Type | Unit | Description |
|---|---|---|---|
| `series_id` | string |  |  |
| `obs_date` | date |  |  |
| `value` | double |  |  |

### silver.fx_rates_daily

Daily FX from FRED: USD per GBP, USD per EUR, and derived GBP per EUR.

| Column | Type | Unit | Description |
|---|---|---|---|
| `fx_date` | date |  |  |
| `usd_per_gbp` | double | GBP |  |
| `usd_per_eur` | double |  |  |
| `gbp_per_eur` | double |  |  |

### silver.gas_prices

Natural gas benchmarks from FRED (USD/MMBtu). Henry Hub daily, EU monthly.

| Column | Type | Unit | Description |
|---|---|---|---|
| `benchmark` | string |  |  |
| `frequency` | string |  |  |
| `obs_date` | date |  |  |
| `price_usd_mmbtu` | double | USD/MMBtu |  |

### silver.gb_demand_forecast

GB national demand forecast (NDF, MW), every published vintage.

| Column | Type | Unit | Description |
|---|---|---|---|
| `publish_time_utc` | timestamp | UTC | When the source published this version or vintage |
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |
| `demand_mw` | double | MW | National demand outturn (INDO) |
| `boundary` | string |  |  |

### silver.gb_demand_forecast_daily

GB national demand forecast 2-14 days ahead (NDFD): forecast daily PEAK demand (MW), every published vintage.

| Column | Type | Unit | Description |
|---|---|---|---|
| `publish_time_utc` | timestamp | UTC | When the source published this version or vintage |
| `forecast_date` | date |  |  |
| `demand_peak_mw` | double | MW |  |

### silver.gb_demand_outturn

GB demand outturn (MW) per settlement period: national (INDO) and transmission system (ITSDO). Both are net of embedded wind and solar.

| Column | Type | Unit | Description |
|---|---|---|---|
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `demand_mw` | double | MW | National demand outturn (INDO) |
| `transmission_demand_mw` | double | MW | Transmission system demand outturn (ITSDO) |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |

### silver.gb_gas_sap_daily

GB System Average Price (SAP) of gas per gas day (05:00-05:00 UK), in p/kWh and GBP/MWh. Source: National Gas Transmission.

| Column | Type | Unit | Description |
|---|---|---|---|
| `gas_day` | date |  | Gas day, 05:00-05:00 UK |
| `sap_p_kwh` | double | p/kWh |  |
| `sap_gbp_mwh` | double | GBP/MWh |  |

### silver.gb_generation_by_fuel

GB half-hourly generation outturn (MW) by fuel type, incl. interconnectors.

| Column | Type | Unit | Description |
|---|---|---|---|
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `fuel_type` | string |  |  |
| `generation_mw` | double | MW |  |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |

### silver.gb_market_index_prices

GB market index price (GBP/MWh) and traded volume per settlement period and provider. APXMIDP carries the trades; N2EXMIDP reports zero volume.

| Column | Type | Unit | Description |
|---|---|---|---|
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `data_provider` | string |  | APXMIDP (carries the trades) or N2EXMIDP (zero volume) |
| `price_gbp_mwh` | double | GBP/MWh |  |
| `volume_mwh` | double | MWh |  |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |

### silver.gb_system_prices

GB system (imbalance) price per settlement period, latest settlement run.

| Column | Type | Unit | Description |
|---|---|---|---|
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `api_start_time_utc` | timestamp | UTC |  |
| `system_price_gbp_mwh` | double | GBP/MWh | Imbalance (system) price; sell and buy prices are equal since 2015 |
| `system_buy_price_gbp_mwh` | double | GBP/MWh |  |
| `net_imbalance_volume_mwh` | double | MWh | Net imbalance volume: positive means the system was short |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |

### silver.gb_wind_forecast

Hourly wind generation forecast (WINDFOR, MW) for transmission-connected wind, every published vintage.

| Column | Type | Unit | Description |
|---|---|---|---|
| `publish_time_utc` | timestamp | UTC | When the source published this version or vintage |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |
| `wind_mw` | double | MW |  |

### silver.gb_wind_solar_forecast

Half-hourly solar, onshore and offshore wind forecasts (DGWS, MW): day-ahead and intraday, every published vintage.

| Column | Type | Unit | Description |
|---|---|---|---|
| `publish_time_utc` | timestamp | UTC | When the source published this version or vintage |
| `process_type` | string |  | Day ahead or Intraday forecast run |
| `psr_type` | string |  | Solar, Wind Onshore or Wind Offshore |
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |
| `forecast_mw` | double | MW |  |
| `document_revision` | int |  |  |

### silver.stock_prices_daily

Daily share prices in GBP (London quotes converted from pence).

| Column | Type | Unit | Description |
|---|---|---|---|
| `ticker` | string |  |  |
| `trade_date` | date |  |  |
| `open_gbp` | double | GBP |  |
| `high_gbp` | double | GBP |  |
| `low_gbp` | double | GBP |  |
| `close_gbp` | double | GBP |  |
| `adj_close_gbp` | double | GBP |  |
| `volume` | bigint |  |  |
| `dividend` | double |  |  |

### silver.ttf_gas_daily

Dutch TTF front-month gas future, daily close (EUR/MWh), trading days only.

| Column | Type | Unit | Description |
|---|---|---|---|
| `trade_date` | date |  |  |
| `close_eur_mwh` | double | EUR/MWh |  |
| `high_eur_mwh` | double | EUR/MWh |  |
| `low_eur_mwh` | double | EUR/MWh |  |

### silver.weather_forecast_gb_hourly

Open-Meteo hourly weather forecasts at 1-degree points over GB, every daily fetch (issued_utc = fetch time).

| Column | Type | Unit | Description |
|---|---|---|---|
| `issued_utc` | timestamp | UTC | When the weather forecast was fetched, or 07:40 UK for archived issues |
| `valid_time_utc` | timestamp | UTC |  |
| `latitude` | double |  |  |
| `longitude` | double |  |  |
| `wind_speed_100m_ms` | double | m/s |  |
| `temp_c` | double | deg C |  |
| `solar_irradiance_wm2` | double | W/m2 |  |

### silver.weather_forecast_gb_hourly_archive

Past forecast issues rebuilt from Open-Meteo's Previous Runs API: what the 07:40 UK fetch would have returned each day. lead_days = how far ahead the forecast was made; lead_shortfall_hours > 0 where it was made after the issue time (late on D+7, as the API keeps 7 days of lead).

| Column | Type | Unit | Description |
|---|---|---|---|
| `issued_utc` | timestamp | UTC | When the weather forecast was fetched, or 07:40 UK for archived issues |
| `valid_time_utc` | timestamp | UTC |  |
| `latitude` | double |  |  |
| `longitude` | double |  |  |
| `wind_speed_100m_ms` | double | m/s |  |
| `temp_c` | double | deg C |  |
| `lead_days` | int |  | Days before valid time the archived forecast was made (1-7) |
| `lead_shortfall_hours` | int | hours | Hours after the issue time this archived value was forecast |

## Gold

Star schema, marts and the week-ahead outlook. Materialized views rebuilt from silver's current rows.

| Table | Description |
|---|---|
| [`dim_company`](#golddim_company) | Energy companies tracked. |
| [`dim_date`](#golddim_date) | Calendar 2020-2035 with the expected number of GB settlement periods per day. |
| [`dim_fx_daily`](#golddim_fx_daily) | Daily FX with weekends/holidays forward-filled from the last published rate. |
| [`dq_settlement_period_completeness`](#golddq_settlement_period_completeness) | Days where loaded GB settlement periods don't match the expected 46/48/50. |
| [`fact_eu_day_ahead_hourly`](#goldfact_eu_day_ahead_hourly) | Hourly day-ahead prices per neighbour zone in EUR and GBP. Where a zone publishes several resolutions, the finest one is used. |
| [`fact_gb_day_ahead_forecasts`](#goldfact_gb_day_ahead_forecasts) | One row per GB settlement period: demand, wind and solar forecasts as known before the delivery day began (latest vintage published before local midnight), so they can be used as model features without look-ahead. The *_adjusted_mw columns correct WINDFOR's overshoot with a factor fitted on days D-365 to D-2. |
| [`fact_gb_half_hourly`](#goldfact_gb_half_hourly) | One row per GB settlement period: price, imbalance, generation mix, weather. |
| [`fact_gb_week_ahead_outlook`](#goldfact_gb_week_ahead_outlook) | Per forecast issue (one a day) and each of the next 7 days: forecast demand, wind, residual demand, weather and the latest gas prices. Inputs for the week-ahead price report (sql/week_ahead_report.sql) and its backtest (sql/week_ahead_backtest.sql). Issues before the live forecast started are rebuilt from Open-Meteo's archive (is_archived_forecast). |
| [`fact_market_daily`](#goldfact_market_daily) | One row per day: GB price stats, neighbour prices, gas, FX, mix and weather. |
| [`fact_stock_returns_daily`](#goldfact_stock_returns_daily) | Daily returns per ticker. |
| [`gb_weather_hourly`](#goldgb_weather_hourly) | GB-average hourly weather (simple mean over the ERA5 grid box). |
| [`gb_wind_power_curve`](#goldgb_wind_power_curve) | Metered GB wind output (MW) by GB-average 100 m wind speed, from the last 365 days of ERA5 and FUELHH. Turns a wind-speed forecast into MW. |
| [`mart_stocks_vs_market`](#goldmart_stocks_vs_market) | Trading days only: each ticker's return next to power and gas moves, plus rolling 60-trading-day correlations. This is the table the dashboard asks about. |

### gold.dim_company

Energy companies tracked.

| Column | Type | Unit | Description |
|---|---|---|---|
| `ticker` | string |  |  |
| `company` | string |  |  |
| `segment` | string |  |  |

### gold.dim_date

Calendar 2020-2035 with the expected number of GB settlement periods per day.

| Column | Type | Unit | Description |
|---|---|---|---|
| `date` | date |  |  |
| `year` | int |  |  |
| `month` | int |  |  |
| `day_name` | string |  |  |
| `is_weekend` | boolean |  |  |
| `expected_settlement_periods` | int |  | 46, 48 or 50, from the clock change |

### gold.dim_fx_daily

Daily FX with weekends/holidays forward-filled from the last published rate.

| Column | Type | Unit | Description |
|---|---|---|---|
| `date` | date |  |  |
| `usd_per_gbp` | double | GBP |  |
| `usd_per_eur` | double |  |  |
| `gbp_per_eur` | double |  |  |

### gold.dq_settlement_period_completeness

Days where loaded GB settlement periods don't match the expected 46/48/50.

| Column | Type | Unit | Description |
|---|---|---|---|
| `date` | date |  |  |
| `expected_settlement_periods` | int |  | 46, 48 or 50, from the clock change |
| `periods_loaded` | bigint |  |  |

### gold.fact_eu_day_ahead_hourly

Hourly day-ahead prices per neighbour zone in EUR and GBP. Where a zone publishes several resolutions, the finest one is used.

| Column | Type | Unit | Description |
|---|---|---|---|
| `zone` | string |  |  |
| `hour_utc` | timestamp | UTC |  |
| `price_eur_mwh` | double | EUR/MWh |  |
| `source_resolution_minutes` | int | minutes |  |
| `gbp_per_eur` | double |  |  |
| `price_gbp_mwh` | double | GBP/MWh |  |

### gold.fact_gb_day_ahead_forecasts

One row per GB settlement period: demand, wind and solar forecasts as known before the delivery day began (latest vintage published before local midnight), so they can be used as model features without look-ahead. The *_adjusted_mw columns correct WINDFOR's overshoot with a factor fitted on days D-365 to D-2.

| Column | Type | Unit | Description |
|---|---|---|---|
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |
| `demand_forecast_mw` | double | MW |  |
| `demand_forecast_published_utc` | timestamp | UTC |  |
| `wind_forecast_mw` | double | MW | Forecast wind: WINDFOR x scale where it covers the hour, else the power curve |
| `wind_forecast_published_utc` | timestamp | UTC |  |
| `solar_forecast_mw` | double | MW |  |
| `wind_onshore_forecast_mw` | double | MW |  |
| `wind_offshore_forecast_mw` | double | MW |  |
| `wind_solar_forecast_published_utc` | timestamp | UTC |  |
| `residual_demand_forecast_mw` | double | MW | Forecast demand minus forecast wind (same definition) |
| `wind_forecast_scale` | double |  | Least-squares factor of metered wind on WINDFOR, days D-365 to D-2 |
| `wind_forecast_adjusted_mw` | double | MW | WINDFOR x wind_forecast_scale |
| `residual_demand_forecast_adjusted_mw` | double | MW |  |

### gold.fact_gb_half_hourly

One row per GB settlement period: price, imbalance, generation mix, weather.

| Column | Type | Unit | Description |
|---|---|---|---|
| `weather_hour_utc` | timestamp | UTC | ERA5 hour joined to the period (ERA5 stamps the end of its hour) |
| `settlement_date` | date |  | GB settlement day (UK local): 46, 48 or 50 half-hour periods |
| `settlement_period` | int |  | Half-hour period of the settlement day, from 1 |
| `start_time_utc` | timestamp | UTC | Start of the period (or hour) |
| `system_price_gbp_mwh` | double | GBP/MWh | Imbalance (system) price; sell and buy prices are equal since 2015 |
| `net_imbalance_volume_mwh` | double | MWh | Net imbalance volume: positive means the system was short |
| `wind_mw` | double | MW |  |
| `gas_mw` | double | MW |  |
| `nuclear_mw` | double | MW |  |
| `biomass_mw` | double | MW |  |
| `coal_mw` | double | MW |  |
| `hydro_and_storage_mw` | double | MW |  |
| `net_imports_mw` | double | MW |  |
| `domestic_generation_mw` | double | MW |  |
| `demand_mw` | double | MW | National demand outturn (INDO) |
| `transmission_demand_mw` | double | MW | Transmission system demand outturn (ITSDO) |
| `market_index_price_gbp_mwh` | double | GBP/MWh | APX market index price; NULL when no volume traded |
| `market_index_volume_mwh` | double | MWh |  |
| `residual_demand_mw` | double | MW | Demand (INDO, net of embedded wind and solar) minus transmission wind |
| `avg_temp_c` | double | deg C | GB-average temperature at 2 m |
| `avg_wind_speed_100m_ms` | double | m/s | GB-average wind speed at 100 m |
| `avg_solar_irradiance_wm2` | double | W/m2 | GB-average surface solar irradiance |
| `wind_share` | double | fraction 0-1 |  |
| `gas_share` | double | fraction 0-1 |  |

### gold.fact_gb_week_ahead_outlook

Per forecast issue (one a day) and each of the next 7 days: forecast demand, wind, residual demand, weather and the latest gas prices. Inputs for the week-ahead price report (sql/week_ahead_report.sql) and its backtest (sql/week_ahead_backtest.sql). Issues before the live forecast started are rebuilt from Open-Meteo's archive (is_archived_forecast).

| Column | Type | Unit | Description |
|---|---|---|---|
| `issue_date` | date |  | UK day the outlook was issued (its weather forecast fetched) |
| `target_date` | date |  | Day being forecast |
| `issued_utc` | timestamp | UTC | When the weather forecast was fetched, or 07:40 UK for archived issues |
| `is_archived_forecast` | boolean |  | Rebuilt from Open-Meteo's Previous Runs API, not a live fetch |
| `wind_forecast_mw` | double | MW | Forecast wind: WINDFOR x scale where it covers the hour, else the power curve |
| `wind_weather_mw` | double | MW | Weather-forecast wind speed through the power curve |
| `windfor_hours` | bigint | hours | Hours of the day covered by Elexon's WINDFOR |
| `avg_wind_speed_100m_ms` | double | m/s | GB-average wind speed at 100 m |
| `avg_temp_c` | double | deg C | GB-average temperature at 2 m |
| `avg_solar_irradiance_wm2` | double | W/m2 | GB-average surface solar irradiance |
| `forecast_lead_shortfall_hours` | int | hours | Most hours any archived value was forecast after the issue |
| `forecast_hours` | bigint | hours | Hours of weather forecast for the day (23, 24 or 25) |
| `demand_peak_forecast_mw` | double | MW | Elexon NDFD forecast of the day's peak demand |
| `demand_forecast_published_utc` | timestamp | UTC |  |
| `demand_avg_to_peak_ratio` | double |  | Average/peak demand over the 60 complete days before issue |
| `latest_sap_gas_day` | date |  | Newest gas day with a SAP known at issue (the day before) |
| `latest_sap_gbp_mwh` | double | GBP/MWh |  |
| `latest_ttf_eur_mwh` | double | EUR/MWh |  |
| `days_ahead` | int |  | target_date minus issue_date, 1 to 7 |
| `is_weekend` | boolean |  |  |
| `demand_avg_forecast_mw` | double | MW | NDFD peak forecast x demand_avg_to_peak_ratio |
| `wind_source` | string |  | Where the day's wind came from: WINDFOR, the power curve, or both |
| `residual_demand_forecast_mw` | double | MW | Forecast demand minus forecast wind (same definition) |

### gold.fact_market_daily

One row per day: GB price stats, neighbour prices, gas, FX, mix and weather.

| Column | Type | Unit | Description |
|---|---|---|---|
| `date` | date |  |  |
| `is_weekend` | boolean |  |  |
| `expected_settlement_periods` | int |  | 46, 48 or 50, from the clock change |
| `gb_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `gb_min_price_gbp_mwh` | double | GBP/MWh |  |
| `gb_max_price_gbp_mwh` | double | GBP/MWh |  |
| `gb_price_volatility` | double |  |  |
| `gb_net_imbalance_mwh` | double | MWh |  |
| `gb_market_index_price_gbp_mwh` | double | GBP/MWh | Daily market index price, volume-weighted over periods |
| `gb_avg_demand_mw` | double | MW |  |
| `gb_peak_demand_mw` | double | MW |  |
| `gb_avg_residual_demand_mw` | double | MW |  |
| `avg_wind_share` | double | fraction 0-1 |  |
| `avg_gas_share` | double | fraction 0-1 |  |
| `avg_temp_c` | double | deg C | GB-average temperature at 2 m |
| `avg_wind_speed_100m_ms` | double | m/s | GB-average wind speed at 100 m |
| `settlement_periods_loaded` | bigint |  | Settlement periods with a system price that day |
| `fr_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `nl_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `be_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `no2_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `dk1_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `ie_sem_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `henry_hub_usd_mmbtu` | double | USD/MMBtu |  |
| `gb_gas_sap_gbp_mwh` | double | GBP/MWh |  |
| `ttf_eur_mwh` | double | EUR/MWh |  |
| `usd_per_gbp` | double | GBP |  |
| `usd_per_eur` | double |  |  |
| `gbp_per_eur` | double |  |  |
| `eu_gas_usd_mmbtu` | double | USD/MMBtu |  |
| `ttf_gbp_mwh` | double | GBP/MWh |  |
| `gb_fr_spread_gbp_mwh` | double | GBP/MWh |  |
| `is_complete_day` | boolean |  | Every expected settlement period is loaded |

### gold.fact_stock_returns_daily

Daily returns per ticker.

| Column | Type | Unit | Description |
|---|---|---|---|
| `ticker` | string |  |  |
| `trade_date` | date |  |  |
| `close_gbp` | double | GBP |  |
| `adj_close_gbp` | double | GBP |  |
| `volume` | bigint |  |  |
| `daily_return` | double |  |  |
| `log_return` | double |  |  |
| `company` | string |  |  |
| `segment` | string |  |  |

### gold.gb_weather_hourly

GB-average hourly weather (simple mean over the ERA5 grid box).

| Column | Type | Unit | Description |
|---|---|---|---|
| `valid_time_utc` | timestamp | UTC |  |
| `avg_temp_c` | double | deg C | GB-average temperature at 2 m |
| `avg_wind_speed_100m_ms` | double | m/s | GB-average wind speed at 100 m |
| `avg_solar_irradiance_wm2` | double | W/m2 | GB-average surface solar irradiance |
| `grid_cells` | bigint |  |  |

### gold.gb_wind_power_curve

Metered GB wind output (MW) by GB-average 100 m wind speed, from the last 365 days of ERA5 and FUELHH. Turns a wind-speed forecast into MW.

| Column | Type | Unit | Description |
|---|---|---|---|
| `wind_speed_bin_ms` | double | m/s | Lower edge of a 0.5 m/s bin of GB-average 100 m wind speed |
| `median_wind_mw` | double | MW | Median metered wind at this wind speed over the last 365 days |
| `half_hours` | bigint | hours |  |

### gold.mart_stocks_vs_market

Trading days only: each ticker's return next to power and gas moves, plus rolling 60-trading-day correlations. This is the table the dashboard asks about.

| Column | Type | Unit | Description |
|---|---|---|---|
| `date` | date |  |  |
| `ticker` | string |  |  |
| `close_gbp` | double | GBP |  |
| `adj_close_gbp` | double | GBP |  |
| `volume` | bigint |  |  |
| `daily_return` | double |  |  |
| `log_return` | double |  |  |
| `company` | string |  |  |
| `segment` | string |  |  |
| `gb_avg_price_gbp_mwh` | double | GBP/MWh |  |
| `gb_price_volatility` | double |  |  |
| `avg_wind_share` | double | fraction 0-1 |  |
| `henry_hub_usd_mmbtu` | double | USD/MMBtu |  |
| `eu_gas_usd_mmbtu` | double | USD/MMBtu |  |
| `gb_fr_spread_gbp_mwh` | double | GBP/MWh |  |
| `gb_price_change` | double |  |  |
| `henry_hub_change` | double |  |  |
| `corr60_return_vs_gb_power` | double |  | 60-trading-day correlation of the return with the GB price change |
| `corr60_return_vs_henry_hub` | double |  | 60-trading-day correlation of the return with the Henry Hub change |
| `obs_in_window` | bigint |  | Returns in the 60-day window |

