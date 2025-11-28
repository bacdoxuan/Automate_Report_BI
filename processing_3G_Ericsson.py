"""
Module for processing 3G Ericsson PowerBI data files.

This module handles:
- Import from Automate_3G_Throughput.xlsx (User_TP_DL sheet)
- Import from Automate_3G_Traffic_User.xlsx (Voice_Erlang, Data_MB, HS_User sheets)
- Transform data to extract max users, busy hour, voice/data traffic
- Merge all data into final result with columns: Ucell ID, Max_user, 3G_Speed, Voice, Data

Usage:
    from processing_3G_Ericsson import Ericsson3GProcessor
    
    processor = Ericsson3GProcessor(download_folder="downloads")
    processor.load_all_3g_data()
    processor.transform_all()
    final_df = processor.merge_final_result()
"""
import os
import sys
import pandas as pd
from pathlib import Path

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class Ericsson3GProcessor:
    """Handle 3G Ericsson data processing for PowerBI reports"""
    
    def __init__(self, download_folder: str = "downloads"):
        """
        Initialize Ericsson3GProcessor
        
        Args:
            download_folder: Path to folder containing downloaded files
        """
        self.download_folder = Path(download_folder)
        self.dataframes = {}
        
    def import_3g_throughput(self) -> pd.DataFrame:
        """
        Import 3G Throughput data from Automate_3G_Throughput.xlsx
        
        Returns:
            DataFrame with User_TP_DL sheet data
        """
        file_path = self.download_folder / "Automate_3G_Throughput.xlsx"
        
        if not file_path.exists():
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        
        print(f"📊 Importing: {file_path.name}")
        print(f"   📄 Sheet: User_TP_DL")
        
        # Define column names explicitly
        columns = ['Date', 'RNC Id', 'RBS Id', 'Ucell Id'] + [str(i) for i in range(24)]
        
        # Read Excel file, skip first row (skiprows=1), use defined columns
        df = pd.read_excel(
            file_path,
            sheet_name='User_TP_DL',
            skiprows=1,
            names=columns,
            usecols=range(28)  # Only read first 28 columns
        )
        
        print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
        
        # Store in dataframes dict
        self.dataframes['User_TP_DL'] = df
        
        return df
    
    def import_3g_traffic_user(self) -> dict:
        """
        Import 3G Traffic User data from Automate_3G_Traffic_User.xlsx
        Imports 3 sheets: Voice_Erlang, Data_MB, HS_User
        
        Returns:
            Dictionary with 3 DataFrames
        """
        file_path = self.download_folder / "Automate_3G_Traffic_User.xlsx"
        
        if not file_path.exists():
            raise FileNotFoundError(f"❌ File not found: {file_path}")
        
        print(f"\n📊 Importing: {file_path.name}")
        
        sheets = ['Voice_Erlang', 'Data_MB', 'HS_User']
        result = {}
        
        for sheet_name in sheets:
            print(f"   📄 Sheet: {sheet_name}")
            
            # Read Excel sheet, skip first row
            df = pd.read_excel(
                file_path,
                sheet_name=sheet_name,
                skiprows=1
            )
            
            print(f"   ✅ Loaded {len(df):,} rows × {len(df.columns)} columns")
            
            # Store in both result dict and main dataframes dict
            result[sheet_name] = df
            self.dataframes[sheet_name] = df
        
        return result
    
    def load_all_3g_data(self) -> dict:
        """
        Load all 3G data files (Throughput + Traffic User)
        
        Returns:
            Dictionary with all loaded DataFrames
        """
        print("=" * 60)
        print("📥 IMPORTING 3G DATA FILES")
        print("=" * 60)
        
        try:
            # Import Throughput data
            self.import_3g_throughput()
            
            # Import Traffic User data
            self.import_3g_traffic_user()
            
            print("\n" + "=" * 60)
            print(f"✅ Successfully loaded {len(self.dataframes)} datasets")
            print("=" * 60)
            
            # Print summary
            print("\n📋 Summary:")
            for name, df in self.dataframes.items():
                print(f"   • {name}: {len(df):,} rows × {len(df.columns)} columns")
            
            return self.dataframes
            
        except Exception as e:
            print(f"\n❌ Error loading data: {str(e)}")
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
    
    # ==================== TRANSFORMATION METHODS ====================
    
    def transform_hs_user(self) -> pd.DataFrame:
        """
        Step 1: Transform HS_User to extract Max_user and Max_hour_index
        
        Returns:
            DataFrame with columns: UCell Id, Max_user, Max_hour_index
        """
        print("\n🔄 Step 1: Transforming HS_User...")
        
        df = self.get_dataframe('HS_User')
        
        # Get hour columns (0 to 23)
        hour_columns = [col for col in df.columns if col not in ['Date', 'RNC Id', 'RBS Id', 'UCell Id']]
        
        # Calculate Max_user: max value across 24 hour columns
        df['Max_user'] = df[hour_columns].max(axis=1)
        
        # Calculate Max_hour_index: column index (0-23) where max occurs
        df['Max_hour_index'] = df[hour_columns].idxmax(axis=1)
        
        # Create result DataFrame with only required columns
        result = df[['UCell Id', 'Max_user', 'Max_hour_index']].copy()
        
        print(f"   ✅ Extracted Max_user and Max_hour_index")
        print(f"   📊 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store transformed data
        self.dataframes['HS_User_Transformed'] = result
        
        return result
    
    def transform_voice_erlang(self) -> pd.DataFrame:
        """
        Step 2: Transform Voice_Erlang to calculate total Voice traffic
        
        Returns:
            DataFrame with columns: UCell Id, Voice
        """
        print("\n🔄 Step 2: Transforming Voice_Erlang...")
        
        df = self.get_dataframe('Voice_Erlang')
        
        # Get hour columns (0 to 23)
        hour_columns = [col for col in df.columns if col not in ['Date', 'RNC Id', 'RBS Id', 'UCell Id']]
        
        # Calculate Voice: sum across 24 hour columns
        result = pd.DataFrame({
            'UCell Id': df['UCell Id'],
            'Voice': df[hour_columns].sum(axis=1)
        })
        
        print(f"   ✅ Calculated Voice traffic sum")
        print(f"   📊 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store transformed data
        self.dataframes['Voice_Erlang_Transformed'] = result
        
        return result
    
    def transform_data_mb(self) -> pd.DataFrame:
        """
        Step 3: Transform Data_MB to calculate total Data traffic
        
        Returns:
            DataFrame with columns: UCell Id, Data
        """
        print("\n🔄 Step 3: Transforming Data_MB...")
        
        df = self.get_dataframe('Data_MB')
        
        # Get hour columns (0 to 23)
        hour_columns = [col for col in df.columns if col not in ['Date', 'RNC Id', 'RBS Id', 'UCell Id']]
        
        # Calculate Data: sum across 24 hour columns
        result = pd.DataFrame({
            'UCell Id': df['UCell Id'],
            'Data': df[hour_columns].sum(axis=1)
        })
        
        print(f"   ✅ Calculated Data traffic sum")
        print(f"   📊 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store transformed data
        self.dataframes['Data_MB_Transformed'] = result
        
        return result
    
    def transform_user_tp_dl(self) -> pd.DataFrame:
        """
        Step 4: Transform User_TP_DL to extract 3G_Speed based on busy hour
        
        Returns:
            DataFrame with columns: Ucell Id, 3G_Speed
        """
        print("\n🔄 Step 4: Transforming User_TP_DL...")
        
        # Get the dataframes
        df_tp = self.get_dataframe('User_TP_DL')
        
        # Get or create HS_User_Transformed if not exists
        if 'HS_User_Transformed' not in self.dataframes:
            hs_transformed = self.transform_hs_user()
        else:
            hs_transformed = self.dataframes['HS_User_Transformed']
        
        # Create lookup dictionary: UCell Id -> Max_hour_index
        hs_lookup = dict(zip(hs_transformed['UCell Id'], hs_transformed['Max_hour_index']))
        
        # Function to get 3G_Speed for each row
        def get_3g_speed(row):
            ucell_id = row['Ucell Id']
            
            # Get the max_hour_index from HS_User
            if ucell_id in hs_lookup:
                hour_index = hs_lookup[ucell_id]
                # Convert to string to match column name
                hour_col = str(hour_index)
                
                # Return the value at that hour column
                if hour_col in row.index:
                    return row[hour_col]
            
            # Return NaN if not found
            return pd.NA
        
        # Apply the function to get 3G_Speed
        result = pd.DataFrame({
            'Ucell Id': df_tp['Ucell Id'],
            '3G_Speed': df_tp.apply(get_3g_speed, axis=1)
        })
        
        print(f"   ✅ Calculated 3G_Speed based on busy hour")
        print(f"   📊 Result: {len(result):,} rows × {len(result.columns)} columns")
        
        # Store transformed data
        self.dataframes['User_TP_DL_Transformed'] = result
        
        return result
    
    def transform_all(self) -> dict:
        """
        Execute all transformation steps
        
        Returns:
            Dictionary with all transformed DataFrames
        """
        print("\n" + "=" * 60)
        print("🔄 TRANSFORMING ALL DATASETS")
        print("=" * 60)
        
        try:
            # Step 1: HS_User
            hs_result = self.transform_hs_user()
            
            # Step 2: Voice_Erlang
            voice_result = self.transform_voice_erlang()
            
            # Step 3: Data_MB
            data_result = self.transform_data_mb()
            
            # Step 4: User_TP_DL
            tp_result = self.transform_user_tp_dl()
            
            print("\n" + "=" * 60)
            print("✅ All transformations completed successfully")
            print("=" * 60)
            
            # Return transformed datasets
            return {
                'HS_User_Transformed': hs_result,
                'Voice_Erlang_Transformed': voice_result,
                'Data_MB_Transformed': data_result,
                'User_TP_DL_Transformed': tp_result
            }
            
        except Exception as e:
            print(f"\n❌ Error during transformation: {str(e)}")
            raise
    
    def merge_final_result(self) -> pd.DataFrame:
        """
        Merge all transformed dataframes into a single final result
        
        Returns:
            DataFrame with columns: Ucell ID, Max_user, 3G_Speed, Voice, Data
        """
        print("\n" + "=" * 60)
        print("🔗 MERGING ALL TRANSFORMED DATA")
        print("=" * 60)
        
        try:
            # Get transformed dataframes
            hs_df = self.dataframes['HS_User_Transformed']
            voice_df = self.dataframes['Voice_Erlang_Transformed']
            data_df = self.dataframes['Data_MB_Transformed']
            tp_df = self.dataframes['User_TP_DL_Transformed']
            
            # Normalize column names (some use 'Ucell Id', others 'UCell Id')
            # Start with HS_User as base (has UCell Id, Max_user, Max_hour_index)
            result = hs_df[['UCell Id', 'Max_user']].copy()
            
            # Merge with User_TP_DL (has Ucell Id, 3G_Speed)
            # Rename Ucell Id to UCell Id for consistency
            tp_df_renamed = tp_df.rename(columns={'Ucell Id': 'UCell Id'})
            result = result.merge(tp_df_renamed[['UCell Id', '3G_Speed']], on='UCell Id', how='left')
            
            # Merge with Voice_Erlang (has UCell Id, Voice)
            result = result.merge(voice_df[['UCell Id', 'Voice']], on='UCell Id', how='left')
            
            # Merge with Data_MB (has UCell Id, Data)
            result = result.merge(data_df[['UCell Id', 'Data']], on='UCell Id', how='left')
            
            # Rename UCell Id to Ucell ID for final output
            result = result.rename(columns={'UCell Id': 'Ucell ID'})
            
            # Reorder columns to match specification
            final_columns = ['Ucell ID', 'Max_user', '3G_Speed', 'Voice', 'Data']
            result = result[final_columns]
            
            print(f"\n✅ Merge completed successfully")
            print(f"📊 Final result: {len(result):,} rows × {len(result.columns)} columns")
            print(f"📋 Columns: {list(result.columns)}")
            
            # Store final result
            self.dataframes['Final_3G_Result'] = result
            
            return result
            
        except Exception as e:
            print(f"\n❌ Error during merge: {str(e)}")
            raise
    
    def standardize_columns(self) -> pd.DataFrame:
        """
        Standardize column names to common format
        
        Returns:
            DataFrame with standardized columns: 3G_Cell_ID, 3G_User, 3G_Speed, 3G_Voice, 3G_Data
        """
        print("\n" + "=" * 60)
        print("🔄 STANDARDIZING COLUMN NAMES")
        print("=" * 60)
        
        try:
            df = self.dataframes['Final_3G_Result'].copy()
            
            # Rename columns to standard format
            df = df.rename(columns={
                'Ucell ID': '3G_Cell_ID',
                'Max_user': '3G_User',
                '3G_Speed': '3G_Speed',  # Already correct
                'Voice': '3G_Voice',
                'Data': '3G_Data'
            })
            
            print(f"✅ Columns standardized")
            print(f"📋 New columns: {list(df.columns)}")
            
            # Store standardized result
            self.dataframes['3G_Standardized'] = df
            
            return df
            
        except Exception as e:
            print(f"\n❌ Error during standardization: {str(e)}")
            raise
    
    def clean_data(self) -> pd.DataFrame:
        """
        Clean data by removing rows with invalid values
        - Remove rows where 3G_Data = 0
        
        Returns:
            DataFrame with cleaned data
        """
        print("\n" + "=" * 60)
        print("🧹 CLEANING DATA")
        print("=" * 60)
        
        try:
            df = self.dataframes['3G_Standardized'].copy()
            original_count = len(df)
            
            # Remove rows where 3G_Data = 0
            df_clean = df[df['3G_Data'] != 0]
            
            removed_count = original_count - len(df_clean)
            
            print(f"✅ Data cleaning completed")
            print(f"📊 Original rows: {original_count:,}")
            print(f"   • Removed {removed_count:,} rows with 3G_Data = 0")
            print(f"📊 Remaining rows: {len(df_clean):,}")
            
            # Store cleaned result
            self.dataframes['3G_Cleaned'] = df_clean
            
            return df_clean
            
        except Exception as e:
            print(f"\n❌ Error during cleaning: {str(e)}")
            raise
    
    def aggregate_by_site(self) -> pd.DataFrame:
        """
        Aggregate cell-level data to site-level data
        - Extract SiteID from 3G_Cell_ID (characters at index 1-6, 0-based)
        - Aggregate: User, Voice, Data by sum; Speed by average
        
        Returns:
            DataFrame aggregated by SiteID: SiteID, 3G_User, 3G_Speed, 3G_Voice, 3G_Data
        """
        print("\n" + "=" * 60)
        print("📊 AGGREGATING DATA BY SITE")
        print("=" * 60)
        
        try:
            df = self.dataframes['3G_Cleaned'].copy()
            
            # Extract SiteID: characters from index 1 to 6 (0-based indexing)
            df['SiteID'] = df['3G_Cell_ID'].astype(str).str[1:7]
            
            print(f"✅ Extracted SiteID from 3G_Cell_ID")
            print(f"📊 Sample SiteIDs: {df['SiteID'].head().tolist()}")
            
            # Aggregate by SiteID
            aggregated = df.groupby('SiteID').agg({
                '3G_User': 'sum',      # Sum of users
                '3G_Speed': 'mean',    # Average speed
                '3G_Voice': 'sum',     # Sum of voice traffic
                '3G_Data': 'sum'       # Sum of data traffic
            }).reset_index()
            
            print(f"\n✅ Aggregation completed")
            print(f"📊 Result: {len(aggregated):,} sites (from {len(df):,} cells)")
            print(f"📋 Columns: {list(aggregated.columns)}")
            
            # Store aggregated result
            self.dataframes['3G_Ericsson_Site_Data'] = aggregated
            
            return aggregated
            
        except Exception as e:
            print(f"\n❌ Error during aggregation: {str(e)}")
            raise


def main():
    """Test the 3G Ericsson data processor"""
    print("🚀 3G ERICSSON DATA PROCESSOR - Testing Module\n")
    
    # Initialize processor
    processor = Ericsson3GProcessor(download_folder="downloads")
    
    # Load all 3G data
    dataframes = processor.load_all_3g_data()
    
    # Execute all transformations
    transformed = processor.transform_all()
    
    # Merge into final result
    final_result = processor.merge_final_result()
    
    # Print final result info
    print("\n" + "=" * 60)
    print("📋 FINAL RESULT INFORMATION")
    print("=" * 60)
    processor.print_dataframe_info('Final_3G_Result')
    
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
    processor.print_dataframe_info('3G_Ericsson_Site_Data')


if __name__ == "__main__":
    main()


