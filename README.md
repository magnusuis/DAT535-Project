# Project Setup and Instructions

This guide provides step-by-step instructions to set up the network, preprocess the data, and run the required analytics and risk calculations.

---

## **Steps to Follow**

### **1. Create Raw Data**
Generate the raw data by running `dataAlteringCustomerData` on the provided dataset.

---

### **2. Set Up the Network**
1. **Configure the Network**:
   - Set up a Hadoop network configuration with **four nodes**: one **namenode** and three **datanodes**.
2. **Configure Spark and Hadoop**:
   - Ensure that Spark and Hadoop are properly configured to work on the network.
3. **Create Data Directories**:
   - Use the following command to create the required directories in HDFS:
     ```bash
     hadoop fs -mkdir -p /data/raw
     hadoop fs -mkdir -p /data/cleaned
     hadoop fs -mkdir -p /data/processed
     ```
4. **Push Files to the Network**:
   - Push the required files to the network, **excluding** `dataAlteringCustomerData` and structured datasets.

---

### **3. Add Raw Data to HDFS**
Place the raw dataset into the `/data/raw` directory using the following command:
```bash
hadoop fs -copyFromLocal /path/to/your/local/file.csv /data/raw/
```

---

### **4. Run Preprocessing**
Run the `clean_data.py` script to clean and preprocess the raw data:
```bash
python clean_data.py
```
---

## **5. Run Data Analytics**
Run the following scripts for data analysis:

- To process contracts:
  ```bash
  python contract.py
  ```

- To generate dashboard data:
  ```bash
  python dashboard.py
  ```

- To calculate monthly charges data:
  ```bash
  python monthlycharge.py
  ```

- To analyze revenue data:
  ```bash
  python revenue_data.py
  ```

---

## **6. Perform Clustering**
Run the K-Means clustering script (also the other ones):
```bash
python KMeanClustering.py
```

---

## **7. Calculate Risk Labels**
Run the `RiskCalculations.py` script to set up risk labels for the dataset:
```bash
python RiskCalculations.py
```

---

## **8. Generate Risk Summary**
Create a Parquet file summarizing the risk labels by running:
```bash
python RiskSummary.py
```
