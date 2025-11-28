"""
Module for processing 4G Ericsson PowerBI data files.

This module handles:
- Import from Automate_North_LTE_Traffic_Data.xlsx (Data_MB, UE_Active_DL, UE_TP_DL sheets)
- Import from Automate_VoLTE_Traffic_Ericsson.xlsx (VoLTE_Traffic sheet)
- Transform data to extract max UE active users, busy hour, voice/data traffic
- Merge all data into final result with columns: Cell ID, max_UE_Active, throughput_max_UE_Active, traffic_VoLTE_4G, traffic_data_4G

Usage:
    from processing_4G_Ericsson import Ericsson4GProcessor
    
    processor = Ericsson4GProcessor(download_folder="downloads")
    processor.load_all_4g_ericsson_data()
    final_df = processor.merge_final_result()
"""
import os
import sys
import pandas as pd
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class Ericsson4GProcessor:
    """Handle 4G Ericsson data processing for PowerBI reports"""
    
    def __init__(self, download_folder: str = "downloads"):
        """
        Initialize Ericsson4GProcessor
        
        Args:
            download_folder: Path to folder containing downloaded files
        """
        self.download_folder = Path(download_folder)
        self.dataframes = {}
        
    def process_data_mb_sheet(self) -> pd.DataFrame:
        """
        Process Data_MB sheet from Automate_North_LTE_Traffic_Data.xlsx
        
        Returns:
            DataFrame with columns: Cell ID, traffic_data_4G
        """
        file_path = self.download_folder / "Automate_North_LTE_Traffic_Data.xlsx"
        
        if not file_path.exists():
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        
        print(f"\n📊 Processing: {file_path.name}")
        print(f"   📄 Sheet: Data_MB")
        
        # Read Excel file with header=None and set column names manually
        df = pd.read_excel(file_path, sheet_name='Data_MB', header=None)
        
        # Set column names: first 3 are Date, Site ID, Cell ID, then 0-23 for hours
        column_names = ['Date', 'Site ID', 'Cell ID'] + [str(i) for i in range(24)]
        df.columns = column_names[:len(df.columns)]  # Only use as many as we have columns
        
        print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
        
        # Get hour columns (0 to 23) - filter columns that are numeric strings
        hour_columns = [col for col in df.columns if col.isdigit() and 0 <= int(col) <= 23]
        hour_columns = sorted(hour_columns, key=int)  # Sort numerically
        
        # Calculate traffic_data_4G: sum across 24 hour columns
        df['traffic_data_4G'] = df[hour_columns].sum(axis=1)
        
        # Keep only required columns
        result = df[['Cell ID', 'traffic_data_4G']].copy()
        
        # Store in dataframes dict
        self.dataframes['Data_MB_Processed'] = result
        
        return result
    
    def process_ue_active_dl_sheet(self) -> pd.DataFrame:
        """
        Process UE_Active_DL sheet from Automate_North_LTE_Traffic_Data.xlsx
        
        Returns:
            DataFrame with columns: Cell ID, max_UE_Active, index_max_UE_Active
        """
        file_path = self.download_folder / "Automate_North_LTE_Traffic_Data.xlsx"
        
        print(f"\n   📄 Sheet: UE_Active_DL")
        
        # Read Excel file with header=None and set column names manually
        df = pd.read_excel(file_path, sheet_name='UE_Active_DL', header=None)
        
        # Set column names: first 3 are Date, Site ID, Cell ID, then 0-23 for hours
        column_names = ['Date', 'Site ID', 'Cell ID'] + [str(i) for i in range(24)]
        df.columns = column_names[:len(df.columns)]  # Only use as many as we have columns
        
        print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
        
        # Get hour columns (0 to 23) - filter columns that are numeric strings
        hour_columns = [col for col in df.columns if col.isdigit() and 0 <= int(col) <= 23]
        hour_columns = sorted(hour_columns, key=int)  # Sort numerically
        
        # Debug: Print available columns and detected hour columns
        # print(f"   🔍 Available columns: {list(df.columns)}")
        # print(f"   🔍 Detected hour columns: {hour_columns}")
        
        if not hour_columns:
            raise ValueError("No hour columns (0-23) found in UE_Active_DL sheet")
        
        # Calculate max_UE_Active: max value across 24 hour columns
        df['max_UE_Active'] = df[hour_columns].max(axis=1)
        
        # Calculate index_max_UE_Active: column index (0-23) where max occurs
        df['index_max_UE_Active'] = df[hour_columns].idxmax(axis=1)
        
        # Keep only required columns
        result = df[['Cell ID', 'max_UE_Active', 'index_max_UE_Active']].copy()
        
        print(f"   📊 Calculated max_UE_Active and index_max_UE_Active")
        print(f"   📋 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store in dataframes dict
        self.dataframes['UE_Active_DL_Processed'] = result
        
        return result
    
    def process_ue_tp_dl_sheet(self) -> pd.DataFrame:
        """
        Process UE_TP_DL sheet from Automate_North_LTE_Traffic_Data.xlsx
        
        Returns:
            DataFrame with columns: Cell ID, throughput_max_UE_Active
        """
        file_path = self.download_folder / "Automate_North_LTE_Traffic_Data.xlsx"
        
        print(f"\n   📄 Sheet: UE_TP_DL")
        
        # Read Excel file with header=None and set column names manually
        df = pd.read_excel(file_path, sheet_name='UE_TP_DL', header=None)
        
        # Set column names: first 3 are Date, Site ID, Cell ID, then 0-23 for hours
        column_names = ['Date', 'Site ID', 'Cell ID'] + [str(i) for i in range(24)]
        df.columns = column_names[:len(df.columns)]  # Only use as many as we have columns
        
        print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
        
        # Get UE_Active_DL processed data
        ue_active_df = self.dataframes['UE_Active_DL_Processed']
        
        # Create lookup dictionary: Cell ID -> index_max_UE_Active
        ue_active_lookup = dict(zip(ue_active_df['Cell ID'], ue_active_df['index_max_UE_Active']))
        
        # Function to get throughput at max UE Active hour
        def get_throughput_at_max_ue(row):
            cell_id = row['Cell ID']
            
            # Get the index_max_UE_Active from UE_Active_DL
            if cell_id in ue_active_lookup:
                hour_index = ue_active_lookup[cell_id]
                hour_col = str(hour_index)
                
                # Return the value at that hour column
                if hour_col in row.index:
                    return row[hour_col]
            
            # Return NaN if not found
            return pd.NA
        
        # Apply the function to get throughput_max_UE_Active
        result = pd.DataFrame({
            'Cell ID': df['Cell ID'],
            'throughput_max_UE_Active': df.apply(get_throughput_at_max_ue, axis=1)
        })
        
        print(f"   📊 Calculated throughput_max_UE_Active (throughput at max UE hour)")
        print(f"   📋 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store in dataframes dict
        self.dataframes['UE_TP_DL_Processed'] = result
        
        return result
    
    def process_volte_traffic_sheet(self) -> pd.DataFrame:
        """
        Process VoLTE_Traffic sheet from Automate_VoLTE_Traffic_Ericsson.xlsx
        
        Returns:
            DataFrame with columns: Cell ID, traffic_VoLTE_4G
        """
        file_path = self.download_folder / "Automate_VoLTE_Traffic_Ericsson.xlsx"
        
        if not file_path.exists():
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        
        print(f"\n📊 Processing: {file_path.name}")
        print(f"   📄 Sheet: VoLTE_Traffic")
        
        # Read Excel file with header=None and set column names manually
        df = pd.read_excel(file_path, sheet_name='VoLTE_Traffic', header=None)
        
        # Set column names: first 3 are Date, Site ID, Cell ID, then 0-23 for hours
        column_names = ['Date', 'Site ID', 'Cell ID'] + [str(i) for i in range(24)]
        df.columns = column_names[:len(df.columns)]  # Only use as many as we have columns
        
        print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
        
        # Get hour columns (0 to 23) - filter columns that are numeric strings
        hour_columns = [col for col in df.columns if col.isdigit() and 0 <= int(col) <= 23]
        hour_columns = sorted(hour_columns, key=int)  # Sort numerically
        
        # Calculate traffic_VoLTE_4G: sum across 24 hour columns
        df['traffic_VoLTE_4G'] = df[hour_columns].sum(axis=1)
        
        # Keep only required columns
        result = df[['Cell ID', 'traffic_VoLTE_4G']].copy()
        
        print(f"   📊 Calculated traffic_VoLTE_4G (sum of 24 hours)")
        print(f"   📋 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store in dataframes dict
        self.dataframes['VoLTE_Traffic_Processed'] = result
        
        return result
    
    def load_all_4g_ericsson_data(self) -> dict:
        """
        Load and process all 4G Ericsson data files
        
        Returns:
            Dictionary with processed DataFrames
        """
        print("=" * 60)
        print("📥 IMPORTING AND PROCESSING 4G ERICSSON DATA FILES")
        print("=" * 60)
        
        try:
            # Process Automate_North_LTE_Traffic_Data.xlsx
            self.process_data_mb_sheet()
            self.process_ue_active_dl_sheet()
            self.process_ue_tp_dl_sheet()
            
            # Process Automate_VoLTE_Traffic_Ericsson.xlsx
            self.process_volte_traffic_sheet()
            
            print("\n" + "=" * 60)
            print(f"✅ Successfully processed {len(self.dataframes)} datasets")
            print("=" * 60)
            
            # Print summary
            print("\n📋 Summary:")
            for name, df in self.dataframes.items():
                print(f"   • {name}: {len(df):,} rows × {len(df.columns)} columns")
            
            return self.dataframes
            
        except Exception as e:
            print(f"\n❌ Error loading data: {str(e)}")
            raise
    
    def merge_final_result(self) -> pd.DataFrame:
        """
        Merge all processed dataframes into final result
        
        Returns:
            DataFrame with columns: Cell ID, max_UE_Active, throughput_max_UE_Active, 
                                   traffic_VoLTE_4G, traffic_data_4G
        """
        print("\n" + "=" * 60)
        print("🔗 MERGING 4G ERICSSON DATA")
        print("=" * 60)
        
        try:
            # Get processed dataframes
            ue_active_df = self.dataframes['UE_Active_DL_Processed']
            ue_tp_df = self.dataframes['UE_TP_DL_Processed']
            volte_df = self.dataframes['VoLTE_Traffic_Processed']
            data_mb_df = self.dataframes['Data_MB_Processed']
            
            # Start with UE_Active_DL (has Cell ID, max_UE_Active, index_max_UE_Active)
            result = ue_active_df[['Cell ID', 'max_UE_Active']].copy()
            
            # Merge with UE_TP_DL (has Cell ID, throughput_max_UE_Active)
            result = result.merge(ue_tp_df, on='Cell ID', how='left')
            
            # Merge with VoLTE_Traffic (has Cell ID, traffic_VoLTE_4G)
            result = result.merge(volte_df, on='Cell ID', how='left')
            
            # Merge with Data_MB (has Cell ID, traffic_data_4G)
            result = result.merge(data_mb_df, on='Cell ID', how='left')
            
            # Reorder columns as specified
            final_columns = [
                'Cell ID',
                'max_UE_Active',
                'throughput_max_UE_Active',
                'traffic_VoLTE_4G',
                'traffic_data_4G'
            ]
            result = result[final_columns]
            
            print(f"\n✅ Merge completed successfully")
            print(f"📊 Final result: {len(result):,} rows × {len(result.columns)} columns")
            print(f"📋 Columns: {list(result.columns)}")
            
            # Store final result
            self.dataframes['4G_ERICSSON_DATA'] = result
            
            return result
            
        except Exception as e:
            print(f"\n❌ Error during merge: {str(e)}")
            raise
    
    def standardize_columns(self) -> pd.DataFrame:
        """
        Standardize column names to common format
        
        Returns:
            DataFrame with standardized columns: 4G_Cell_ID, 4G_User, 4G_Speed, 4G_Voice, 4G_Data
        """
        print("\n" + "=" * 60)
        print("🔄 STANDARDIZING COLUMN NAMES")
        print("=" * 60)
        
        try:
            df = self.dataframes['4G_ERICSSON_DATA'].copy()
            
            # Rename columns to standard format
            df = df.rename(columns={
                'Cell ID': '4G_Cell_ID',
                'max_UE_Active': '4G_User',
                'throughput_max_UE_Active': '4G_Speed',
                'traffic_VoLTE_4G': '4G_Voice',
                'traffic_data_4G': '4G_Data'
            })
            
            print(f"✅ Columns standardized")
            print(f"📋 New columns: {list(df.columns)}")
            
            # Store standardized result
            self.dataframes['4G_Standardized'] = df
            
            return df
            
        except Exception as e:
            print(f"\n❌ Error during standardization: {str(e)}")
            raise
    
    def clean_data(self) -> pd.DataFrame:
        """
        Clean data by removing rows with invalid values
        - Remove rows where 4G_Data = 0
        
        Returns:
            DataFrame with cleaned data
        """
        print("\n" + "=" * 60)
        print("🧹 CLEANING DATA")
        print("=" * 60)
        
        try:
            df = self.dataframes['4G_Standardized'].copy()
            original_count = len(df)
            
            # Remove rows where 4G_Data = 0
            df_clean = df[df['4G_Data'] != 0]
            
            removed_count = original_count - len(df_clean)
            
            print(f"✅ Data cleaning completed")
            print(f"📊 Original rows: {original_count:,}")
            print(f"   • Removed {removed_count:,} rows with 4G_Data = 0")
            print(f"📊 Remaining rows: {len(df_clean):,}")
            
            # Store cleaned result
            self.dataframes['4G_Cleaned'] = df_clean
            
            return df_clean
            
        except Exception as e:
            print(f"\n❌ Error during cleaning: {str(e)}")
            raise
    
    def aggregate_by_site(self) -> pd.DataFrame:
        """
        Aggregate cell-level data to site-level data
        - Extract SiteID from 4G_Cell_ID (characters at index 1-6, 0-based)
        - Aggregate: User, Voice, Data by sum; Speed by average
        
        Returns:
            DataFrame aggregated by SiteID: SiteID, 4G_User, 4G_Speed, 4G_Voice, 4G_Data
        """
        print("\n" + "=" * 60)
        print("📊 AGGREGATING DATA BY SITE")
        print("=" * 60)
        
        try:
            df = self.dataframes['4G_Cleaned'].copy()
            
            # Extract SiteID: characters from index 1 to 6 (0-based indexing)
            df['SiteID'] = df['4G_Cell_ID'].astype(str).str[1:7]
            
            print(f"✅ Extracted SiteID from 4G_Cell_ID")
            print(f"📊 Sample SiteIDs: {df['SiteID'].head().tolist()}")
            
            # Aggregate by SiteID
            aggregated = df.groupby('SiteID').agg({
                '4G_User': 'sum',      # Sum of users
                '4G_Speed': 'mean',    # Average speed
                '4G_Voice': 'sum',     # Sum of voice traffic
                '4G_Data': 'sum'       # Sum of data traffic
            }).reset_index()
            
            print(f"\n✅ Aggregation completed")
            print(f"📊 Result: {len(aggregated):,} sites (from {len(df):,} cells)")
            print(f"📋 Columns: {list(aggregated.columns)}")
            
            # Store aggregated result
            self.dataframes['4G_Ericsson_Site_Data'] = aggregated
            
            return aggregated
            
        except Exception as e:
            print(f"\n❌ Error during aggregation: {str(e)}")
            raise
    
    def get_dataframe(self, name: str) -> pd.DataFrame:
        """
        Get a specific dataframe by name
        
        Args:
            name: Name of the dataframe
            
        Returns:
            DataFrame
        """
        if name not in self.dataframes:
            raise KeyError(f"DataFrame '{name}' not found. Available: {list(self.dataframes.keys())}")
        
        return self.dataframes[name]
    
    def print_dataframe_info(self, name: str):
        """
        Print detailed information about a dataframe
        
        Args:
            name: Name of the dataframe
        """
        df = self.get_dataframe(name)
        
        print(f"\n{'=' * 60}")
        print(f"📊 DataFrame: {name}")
        print(f"{'=' * 60}")
        print(f"Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")
        print(f"\nColumns: {list(df.columns)}")
        print(f"\nData types:\n{df.dtypes}")
        print(f"\nFirst 5 rows:\n{df.head()}")
        print(f"\nMemory usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")


def main():
    """Test the 4G Ericsson data processor"""
    print("🚀 4G ERICSSON DATA PROCESSOR - Testing Module\n")
    
    # Initialize processor
    processor = Ericsson4GProcessor(download_folder="downloads")
    
    # Load all 4G Ericsson data
    dataframes = processor.load_all_4g_ericsson_data()
    
    # Merge into final result
    final_result = processor.merge_final_result()
    
    # Print final result info
    print("\n" + "=" * 60)
    print("📋 FINAL RESULT INFORMATION")
    print("=" * 60)
    processor.print_dataframe_info('4G_ERICSSON_DATA')
    
    # Standardize column names
    standardized = processor.standardize_columns()
    
    # Clean data (remove invalid values)
    cleaned = processor.clean_data()
    
    # Aggregate by site
    site_data = processor.aggregate_by_site()
    
    # Print site-level data info
    print("\n" + "=" * 60)
    print("📋 SITE-LEVEL DATA INFORMATION")
    print("=" * 60)
    processor.print_dataframe_info('4G_Ericsson_Site_Data')


if __name__ == "__main__":
    main()
