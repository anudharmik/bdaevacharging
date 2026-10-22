# Dataset Specification: Electric Vehicle Charging Transactions

## 1. Overview & Source Attribution

- **Dataset Title**: *A dataset for multi-faceted analysis of electric vehicle charging transactions*
- **Peer-Reviewed Reference**: Nature Scientific Data (2024), DOI: [10.1038/s41597-024-02942-9](https://doi.org/10.1038/s41597-024-02942-9)
- **IEEE PES Portal Reference**: [IEEE Task Force on Data Sharing in Energy Systems](https://ieee-pes-data-sharing.org/datasets/detail/c903145b-3ba7-4fd3-ac1b-7727abf79e73)
- **Primary Data Repository**: [Figshare Dataset Repository](https://figshare.com/articles/dataset/A_dataset_for_multi-faceted_analysis_of_electric_vehicle_charging_transactions/22495141) (DOI: `10.6084/m9.figshare.22495141`)
- **Authors**: Keon Baek, Eunjung Lee, Jinho Kim
- **License**: Creative Commons Attribution 4.0 International (CC BY 4.0)
- **Data Collection Period**: September 30, 2021 – September 30, 2022 (One full annual commercial operating cycle)
- **Scale**: Exactly **72,856 verified charging sessions**, 2,337 EV users, and 2,119 chargers.

---

## 2. File Information & Download Procedure

- **Target File**: `ChargingRecords.csv`
- **File Size**: 7,605,859 bytes (~7.6 MB)
- **Target Local Destination**: `data/raw/ChargingRecords.csv`

### Manual Download Steps
1. Navigate in your browser to the Figshare repository: [https://figshare.com/articles/dataset/A_dataset_for_multi-faceted_analysis_of_electric_vehicle_charging_transactions/22495141](https://figshare.com/articles/dataset/A_dataset_for_multi-faceted_analysis_of_electric_vehicle_charging_transactions/22495141).
2. Locate the file named `ChargingRecords.csv` (size: ~7.6 MB).
3. Click the **Download** button for `ChargingRecords.csv`.
4. Place the downloaded file into the project directory at:
   ```
   e:\BDA ev charge\data\raw\ChargingRecords.csv
   ```

### Direct Download Command (Optional / Automated)
The Figshare direct file asset identifier is `39952252`:
```powershell
curl.exe -L "https://ndownloader.figshare.com/files/39952252" -o "data/raw/ChargingRecords.csv"
```

---

## 3. Schema & Data Dictionary

The file contains 16 columns recorded per charging session:

| Column Name | Data Type (Raw) | PySpark Target Type | Description & Semantic Meaning |
| :--- | :--- | :--- | :--- |
| `ChargingSessionID` | Integer / String | `StringType` / `LongType` | Unique identifier for each independent charging transaction session. |
| `UserID` | String | `StringType` | Anonymized unique identifier for the EV driver/customer. |
| `ChargerID` | String | `StringType` | Unique identifier for the charging pile/station unit. |
| `ChargerCompany` | String | `StringType` | Name or code of the charging network operator company. |
| `Location` | String | `StringType` | Geographic category / facility type (e.g., apartment, public, commercial parking, workplace). |
| `ChargerType` | String | `StringType` | Classifier for charger configuration (e.g., single-port, dual-port, high-voltage). |
| `ChargerCapacity` | Float / String | `DoubleType` | Rated charging capacity in kilowatts (kW) (e.g., 7.0 kW, 50.0 kW, 100.0 kW). |
| `ChargerACDC` | String | `StringType` | Current mode: `AC` (slow charging, Level 2) or `DC` (rapid/fast direct current charging). |
| `StartDay` | String | `DateType` | Date when charging commenced (`YYYY-MM-DD`). |
| `StartTime` | String | `StringType` | Time of day when charging commenced (`HH:MM:SS`). |
| `EndDay` | String | `DateType` | Date when charging terminated (`YYYY-MM-DD`). |
| `EndTime` | String | `StringType` | Time of day when charging terminated (`HH:MM:SS`). |
| `StartDatetime` | String | `TimestampType` | Full timestamp of session initiation (`YYYY-MM-DD HH:MM:SS`). |
| `EndDatetime` | String | `TimestampType` | Full timestamp of session termination (`YYYY-MM-DD HH:MM:SS`). |
| `Duration` | Float / String | `DoubleType` | Session duration in hours or minutes. |
| `Demand` | Float / String | `DoubleType` | Total energy delivered during the session in kilowatt-hours (kWh). |

---

## 4. Planned Analytical Aggregations (Phase 2 Preview)

Using PySpark distributed transformations:
1. **Time-of-Use & Hourly Demand Profile**: Energy consumption aggregated across the 24 hours of the day (peak vs. off-peak grid load analysis).
2. **Charger AC vs. DC Utilization**: Total kWh delivered, average session duration, and utilization rates across AC and DC chargers.
3. **Location Category Breakdown**: Energy demand patterns by installation category (residential, public highway, workplace).
4. **Station Performance & Efficiency**: Throughput (kWh/hour), utilization rate, and downtime per `ChargerID`.
5. **User Behavior Distribution**: Charging frequency, average energy per session, and clustering of user habits.
