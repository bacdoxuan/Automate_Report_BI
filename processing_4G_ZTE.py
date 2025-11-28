"""
Module for processing 4G ZTE PowerBI data files.

This module handles:
- Find and import 4 CSV files with pattern matching:
  * Automate_4G_ZTE_Traffic_EMS1_WD_*.csv
  * Automate_4G_ZTE_Traffic_EMS2_WD_*.csv
  * Automate_4G_ZTE_User_TP_EMS1_BH_*.csv
  * Automate_4G_ZTE_User_TP_EMS2_BH_*.csv
- Extract and append traffic data from EMS1 and EMS2
- Extract and append user throughput data from EMS1 and EMS2
- Merge into final result with columns: Cell Name, Average DL Active User Number, DL_THP_PER_USER(kbps), [LTE]Average Number of QCI1(Traffic)(Erl), Data(MB)

Usage:
    from processing_4G_ZTE import ZTE4GProcessor
    
    processor = ZTE4GProcessor(download_folder="downloads")
    processor.load_all_4g_zte_data()
    final_df = processor.merge_final_result()
"""
import os
import sys
import pandas as pd
from pathlib import Path
from glob import glob

# Set UTF-8 encoding for Windows console
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')


class ZTE4GProcessor:
    """Handle 4G ZTE data processing for PowerBI reports"""
    
    def __init__(self, download_folder: str = "downloads"):
        """
        Initialize ZTE4GProcessor
        
        Args:
            download_folder: Path to folder containing downloaded CSV files
        """
        self.download_folder = Path(download_folder)
        self.dataframes = {}
        
    def find_csv_file(self, pattern: str) -> Path:
        """
        Find CSV file matching the pattern
        
        Args:
            pattern: File name pattern to match
            
        Returns:
            Path to the found file
            
        Raises:
            FileNotFoundError: If no file matches the pattern
        """
        search_pattern = str(self.download_folder / f"{pattern}*.csv")
        matches = glob(search_pattern)
        
        if not matches:
            raise FileNotFoundError(f"❌ No file found matching pattern: {pattern}")
        
        # Return the first match (assuming one file per pattern)
        return Path(matches[0])
    
    def load_traffic_files(self) -> pd.DataFrame:
        """
        Load and merge traffic files from EMS1 and EMS2
        
        Returns:
            DataFrame with columns: Cell Name, Data(MB), [LTE]Average Number of QCI1(Traffic)(Erl)
        """
        print("\n📊 Loading 4G ZTE Traffic files...")
        
        # Define patterns
        patterns = [
            "Automate_4G_ZTE_Traffic_EMS1_WD_",
            "Automate_4G_ZTE_Traffic_EMS2_WD_"
        ]
        
        dfs = []
        
        for pattern in patterns:
            try:
                file_path = self.find_csv_file(pattern)
                print(f"   📄 Found: {file_path.name}")
                
                # Load CSV, skip first row, select required columns
                df = pd.read_csv(
                    file_path,
                    skiprows=1,
                    usecols=['Cell Name', 'Data(MB)', '[LTE]Average Number of QCI1(Traffic)(Erl)']
                )
                
                print(f"      ✅ Loaded {len(df):,} rows")
                dfs.append(df)
                
            except Exception as e:
                print(f"   ⚠️ Error loading {pattern}: {str(e)}")
        
        # Concatenate all dataframes
        if dfs:
            result = pd.concat(dfs, ignore_index=True)
            print(f"\n   📊 Combined Traffic data: {len(result):,} rows × {len(result.columns)} columns")
            
            # Store in dataframes dict
            self.dataframes['4G_ZTE_TRAFFIC'] = result
            return result
        else:
            raise ValueError("❌ No traffic files were loaded")
    
    def load_user_throughput_files(self) -> pd.DataFrame:
        """
        Load and merge user throughput files from EMS1 and EMS2
        
        Returns:
            DataFrame with columns: Cell Name, DL_THP_PER_USER(kbps), Average DL Active User Number
        """
        print("\n📊 Loading 4G ZTE User Throughput files...")
        
        # Define patterns
        patterns = [
            "Automate_4G_ZTE_User_TP_EMS1_BH_",
            "Automate_4G_ZTE_User_TP_EMS2_BH_"
        ]
        
        dfs = []
        
        for pattern in patterns:
            try:
                file_path = self.find_csv_file(pattern)
                print(f"   📄 Found: {file_path.name}")
                
                # Load CSV, skip first row, select required columns
                df = pd.read_csv(
                    file_path,
                    skiprows=1,
                    usecols=['Cell Name', 'DL_THP_PER_USER(kbps)', 'Average DL Active User Number']
                )
                
                print(f"      ✅ Loaded {len(df):,} rows")
                dfs.append(df)
                
            except Exception as e:
                print(f"   ⚠️ Error loading {pattern}: {str(e)}")
        
        # Concatenate all dataframes
        if dfs:
            result = pd.concat(dfs, ignore_index=True)
            print(f"\n   📊 Combined User Throughput data: {len(result):,} rows × {len(result.columns)} columns")
            
            # Store in dataframes dict
            self.dataframes['4G_ZTE_USER_THROUGHPUT'] = result
            return result
        else:
            raise ValueError("❌ No user throughput files were loaded")
    
    def load_all_4g_zte_data(self) -> dict:
        """
        Load all 4G ZTE data files
        
        Returns:
            Dictionary with loaded DataFrames
        """
        print("=" * 60)
        print("📥 IMPORTING 4G ZTE DATA FILES")
        print("=" * 60)
        
        try:
            # Load traffic data
            self.load_traffic_files()
            
            # Load user throughput data
            self.load_user_throughput_files()
            
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
    
    def merge_final_result(self) -> pd.DataFrame:
        """
        Merge traffic and user throughput dataframes into final result
        
        Returns:
            DataFrame with columns: Cell Name, Average DL Active User Number, DL_THP_PER_USER(kbps), 
                                   [LTE]Average Number of QCI1(Traffic)(Erl), Data(MB)
        """
        print("\n" + "=" * 60)
        print("🔗 MERGING 4G ZTE DATA")
        print("=" * 60)
        
        try:
            # Get loaded dataframes
            traffic_df = self.dataframes['4G_ZTE_TRAFFIC']
            user_tp_df = self.dataframes['4G_ZTE_USER_THROUGHPUT']
            
            # Merge on Cell Name
            result = user_tp_df.merge(
                traffic_df,
                on='Cell Name',
                how='left'
            )
            
            # Reorder columns as specified
            final_columns = [
                'Cell Name',
                'Average DL Active User Number',
                'DL_THP_PER_USER(kbps)',
                '[LTE]Average Number of QCI1(Traffic)(Erl)',
                'Data(MB)'
            ]
            result = result[final_columns]
            
            print(f"\n✅ Merge completed successfully")
            print(f"📊 Final result: {len(result):,} rows × {len(result.columns)} columns")
            print(f"📋 Columns: {list(result.columns)}")
            
            # Store final result
            self.dataframes['4G_ZTE_DATA'] = result
            
            return result
            
        except Exception as e:
            print(f"\n❌ Error during merge: {str(e)}")
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
    """Test the 4G ZTE data processor"""
    print("🚀 4G ZTE DATA PROCESSOR - Testing Module\n")
    
    # Initialize processor
    processor = ZTE4GProcessor(download_folder="downloads")
    
    # Load all 4G ZTE data
    dataframes = processor.load_all_4g_zte_data()
    
    # Merge into final result
    final_result = processor.merge_final_result()
    
    # Print final result info
    print("\n" + "=" * 60)
    print("📋 FINAL RESULT INFORMATION")
    print("=" * 60)
    processor.print_dataframe_info('4G_ZTE_DATA')


if __name__ == "__main__":
    main()
