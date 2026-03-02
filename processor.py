import pandas as pd
import glob
import os
from pathlib import Path
import re

class MigrationFlowProcessor:
    """
    Custom processor for Census Bureau migration flow CSV files
    Preserves FIPS codes as strings with leading zeros
    Removes footnote rows at the bottom of files
    """
    
    def __init__(self, input_folder, output_folder=None):
        self.input_folder = input_folder
        self.output_folder = output_folder or input_folder + "_processed"
        
    def extract_headers(self, file_path):
        """Extract and understand the header structure"""
        # Read the first 5 rows to understand headers
        header_df = pd.read_csv(file_path, header=None, nrows=5)
        
        # Row 2 (index 1) contains main column descriptions
        # Row 3 (index 2) contains sub-categories
        # Row 4 (index 3) contains Estimate/MOE indicators
        
        main_headers = header_df.iloc[1].fillna('').astype(str).tolist()
        sub_headers = header_df.iloc[2].fillna('').astype(str).tolist()
        data_types = header_df.iloc[3].fillna('').astype(str).tolist()
        
        column_names = []
        
        # First 6 columns - location identifiers (fixed names)
        location_cols = [
            'origin_fips_state',
            'origin_fips_county',
            'dest_fips_state',
            'dest_fips_county',
            'origin_state_name',
            'origin_county_name'
        ]
        column_names.extend(location_cols)
        
        # Track current category for building names
        current_category = None
        
        # Process columns 6-19 (origin statistics)
        for i in range(6, 20):
            main = main_headers[i] if i < len(main_headers) else ''
            sub = sub_headers[i] if i < len(sub_headers) else ''
            dtype = data_types[i] if i < len(data_types) else ''
            
            # Build descriptive name
            if 'nonmovers' in str(sub).lower():
                base = 'origin_nonmovers'
            elif 'movers within united states' in str(sub).lower():
                base = 'origin_movers_us'
            elif 'movers within same county' in str(sub).lower():
                base = 'origin_movers_same_county'
            elif 'movers from different county, same state' in str(sub).lower():
                base = 'origin_movers_diff_county_same_state'
            elif 'movers from different state' in str(sub).lower():
                base = 'origin_movers_diff_state'
            elif 'movers from abroad' in str(sub).lower():
                base = 'origin_movers_abroad'
            elif 'population' in str(main).lower():
                base = 'origin_population'
            else:
                # Fallback - use a cleaned version of the header
                base = re.sub(r'[^a-zA-Z0-9]', '_', str(main or sub)).lower().strip('_')
                if not base:
                    base = f'origin_col_{i}'
            
            # Add estimate/moe suffix
            if 'estimate' in str(dtype).lower():
                column_names.append(f"{base}_estimate")
            elif 'moe' in str(dtype).lower():
                column_names.append(f"{base}_moe")
            else:
                column_names.append(base)
        
        # Columns 20-21 - destination state and county
        column_names.append('dest_state_name')
        column_names.append('dest_county_name')
        
        # Process columns 22-37 (destination statistics)
        for i in range(22, 38):
            main = main_headers[i] if i < len(main_headers) else ''
            sub = sub_headers[i] if i < len(sub_headers) else ''
            dtype = data_types[i] if i < len(data_types) else ''
            
            # Build descriptive name
            if 'nonmovers' in str(sub).lower():
                base = 'dest_nonmovers'
            elif 'movers within united states' in str(sub).lower():
                base = 'dest_movers_us'
            elif 'movers within same county' in str(sub).lower():
                base = 'dest_movers_same_county'
            elif 'movers to different county, same state' in str(sub).lower():
                base = 'dest_movers_diff_county_same_state'
            elif 'movers to different state' in str(sub).lower():
                base = 'dest_movers_diff_state'
            elif 'movers to puerto rico' in str(sub).lower():
                base = 'dest_movers_pr'
            elif 'population' in str(main).lower():
                base = 'dest_population'
            else:
                # Fallback
                base = re.sub(r'[^a-zA-Z0-9]', '_', str(main or sub)).lower().strip('_')
                if not base:
                    base = f'dest_col_{i}'
            
            # Add estimate/moe suffix
            if 'estimate' in str(dtype).lower():
                column_names.append(f"{base}_estimate")
            elif 'moe' in str(dtype).lower():
                column_names.append(f"{base}_moe")
            else:
                column_names.append(base)
        
        return column_names
    
    def format_fips_code(self, value, length=3):
        """
        Format FIPS codes to ensure they have leading zeros and are strings
        """
        if pd.isna(value) or value == '' or value == '-':
            return None
        try:
            # Convert to string and pad with leading zeros
            return str(int(float(value))).zfill(length)
        except (ValueError, TypeError):
            # If it's already a string with special codes (ASI, EUR, etc.), return as is
            return str(value)
    
    def is_footnote_row(self, row):
        """
        Check if a row is a footnote row by looking for common footnote indicators
        """
        # Convert first few columns to string for checking
        first_col = str(row.iloc[0]) if len(row) > 0 else ''
        second_col = str(row.iloc[1]) if len(row) > 1 else ''
        
        # Check for footnote indicators
        footnote_keywords = ['footnote', 'source', 'margin of error', 'incudes', 'moes based']
        
        # Check if first column contains footnote keywords (case insensitive)
        if any(keyword in first_col.lower() for keyword in footnote_keywords):
            return True
            
        # Check if first column is empty and second column has a footnote-like value
        if (first_col == '' or first_col == 'nan' or pd.isna(row.iloc[0])) and \
           any(keyword in second_col.lower() for keyword in footnote_keywords):
            return True
            
        # Check for specific patterns in your data
        if first_col == 'Footnotes:' or first_col == 'Footnotes':
            return True
            
        # Check if the row has very few non-null values compared to header length
        # (footnote rows typically have data only in first few columns)
        non_null_count = row.count()
        if non_null_count < 5:  # If most columns are empty, it's probably a footnote
            # But make sure it's not just an empty data row
            # Check if the first column has actual data-like content
            if first_col and not any(keyword in first_col.lower() for keyword in footnote_keywords):
                # If it has content but not footnote keywords, might be valid data
                return False
            return True
            
        return False
    
    def process_file(self, file_path, output_path=None):
        """
        Process a single migration flow file
        """
        print(f"\nProcessing: {os.path.basename(file_path)}")
        
        try:
            # Extract column names from headers
            column_names = self.extract_headers(file_path)
            print(f"  Detected {len(column_names)} columns")
            
            # Print first few column names to verify
            print("  Sample columns:")
            for name in column_names[:10]:
                print(f"    {name}")
            
            # Read the data starting from row 5 (index 4)
            # Keep everything as string initially to preserve formatting
            df = pd.read_csv(file_path, header=None, skiprows=4, dtype=str)
            
            # Ensure we have the right number of columns
            if len(df.columns) > len(column_names):
                df = df.iloc[:, :len(column_names)]
            elif len(df.columns) < len(column_names):
                for i in range(len(df.columns), len(column_names)):
                    df[i] = pd.NA
            
            # Assign column names
            df.columns = column_names
            
            # NEW: Remove footnote rows
            initial_row_count = len(df)
            
            # Apply footnote filtering
            footnote_mask = df.apply(self.is_footnote_row, axis=1)
            df = df[~footnote_mask].copy()
            
            footnote_count = initial_row_count - len(df)
            if footnote_count > 0:
                print(f"  Removed {footnote_count} footnote rows")
            
            # Format FIPS codes to preserve leading zeros
            fips_columns = ['origin_fips_state', 'origin_fips_county', 
                           'dest_fips_state', 'dest_fips_county']
            
            for col in fips_columns:
                if col in df.columns:
                    if 'state' in col:
                        # State FIPS are 2 digits
                        df[col] = df[col].apply(lambda x: self.format_fips_code(x, 2))
                    else:
                        # County FIPS are 3 digits
                        df[col] = df[col].apply(lambda x: self.format_fips_code(x, 3))
            
            # Convert estimate and MOE columns to numeric
            for col in df.columns:
                if col.endswith('_estimate') or col.endswith('_moe') or 'population' in col:
                    if col not in fips_columns:  # Don't convert FIPS codes
                        try:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                        except:
                            pass
            
            # Clean up county names
            if 'origin_county_name' in df.columns:
                df['origin_county_name_clean'] = df['origin_county_name'].apply(
                    lambda x: None if pd.isna(x) or x == '-' else str(x).replace(' County', '')
                )
            
            if 'dest_county_name' in df.columns:
                df['dest_county_name_clean'] = df['dest_county_name'].apply(
                    lambda x: None if pd.isna(x) or x == '-' else str(x).replace(' County', '')
                )
            
            # Create location keys for easier joining
            df['origin_location_key'] = df.apply(
                lambda row: f"{row['origin_fips_state']}_{row['origin_fips_county']}" 
                if pd.notna(row.get('origin_fips_county')) and row.get('origin_fips_county') not in ['000', None]
                else row.get('origin_state_name'), axis=1
            )
            
            df['dest_location_key'] = df.apply(
                lambda row: f"{row['dest_fips_state']}_{row['dest_fips_county']}"
                if pd.notna(row.get('dest_fips_county')) and row.get('dest_fips_county') not in ['000', None]
                else row.get('dest_state_name'), axis=1
            )
            
            # Add metadata
            df['source_file'] = os.path.basename(file_path)
            
            year_match = re.search(r'inflow_(\d+)_(\d+)', os.path.basename(file_path))
            if year_match:
                df['year_start'] = f"20{year_match.group(1)}"
                df['year_end'] = f"20{year_match.group(2)}"
            
            # Save the processed file
            if output_path is None:
                filename = os.path.basename(file_path).replace('.csv', '_processed.csv')
                output_path = os.path.join(self.output_folder, filename)
            
            Path(os.path.dirname(output_path)).mkdir(parents=True, exist_ok=True)
            df.to_csv(output_path, index=False)
            
            print(f"  ✓ Saved {len(df):,} records with {len(df.columns)} columns")
            
            # Show sample of FIPS codes to verify formatting
            print("\n  Sample FIPS codes (should have leading zeros):")
            for col in fips_columns:
                if col in df.columns:
                    sample = df[col].dropna().iloc[:3].tolist() if len(df) > 0 else []
                    print(f"    {col}: {sample}")
            
            return df
            
        except Exception as e:
            print(f"  ✗ Error: {str(e)}")
            import traceback
            traceback.print_exc()
            return None
    
    def process_all_files(self):
        """Process all CSV files in the input folder"""
        Path(self.output_folder).mkdir(parents=True, exist_ok=True)
        
        csv_files = glob.glob(os.path.join(self.input_folder, "*.csv"))
        print(f"Found {len(csv_files)} CSV files to process")
        
        if len(csv_files) == 0:
            print("No CSV files found!")
            return
        
        results = {'success': [], 'failed': []}
        
        for i, file_path in enumerate(csv_files, 1):
            print(f"\n[{i}/{len(csv_files)}]", end="")
            df = self.process_file(file_path)
            
            if df is not None:
                results['success'].append(file_path)
            else:
                results['failed'].append(file_path)
        
        # Print summary
        print(f"\n{'='*60}")
        print(f"PROCESSING COMPLETE")
        print(f"{'='*60}")
        print(f"Successfully processed: {len(results['success'])} files")
        print(f"Failed: {len(results['failed'])} files")
        
        return results


# Simpler approach with proper FIPS handling
def simple_process_with_fips(input_folder, output_folder):
    """
    Simple processor that properly handles FIPS codes as strings
    """
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    print(f"Found {len(csv_files)} files")
    
    def format_fips(val, digits):
        """Format FIPS code with leading zeros"""
        if pd.isna(val) or val == '' or val == '-':
            return None
        try:
            return str(int(float(val))).zfill(digits)
        except:
            return str(val)
    
    for i, file_path in enumerate(csv_files, 1):
        filename = os.path.basename(file_path)
        print(f"\rProcessing {i}/{len(csv_files)}: {filename}", end="")
        
        try:
            # Read with pandas default header detection
            df = pd.read_csv(file_path, skiprows=4, dtype=str)
            
            # Clean up column names
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
            # Identify FIPS columns (usually the first 4 columns)
            if len(df.columns) >= 4:
                # Format state FIPS (2 digits)
                df.iloc[:, 0] = df.iloc[:, 0].apply(lambda x: format_fips(x, 2))
                df.iloc[:, 2] = df.iloc[:, 2].apply(lambda x: format_fips(x, 2))
                
                # Format county FIPS (3 digits)
                df.iloc[:, 1] = df.iloc[:, 1].apply(lambda x: format_fips(x, 3))
                df.iloc[:, 3] = df.iloc[:, 3].apply(lambda x: format_fips(x, 3))
            
            # Add metadata
            df['source_file'] = filename
            
            year_match = re.search(r'inflow_(\d+)_(\d+)', filename)
            if year_match:
                df['year_start'] = f"20{year_match.group(1)}"
                df['year_end'] = f"20{year_match.group(2)}"
            
            # Save
            output_path = os.path.join(output_folder, filename)
            df.to_csv(output_path, index=False)
            
        except Exception as e:
            print(f"\nError on {filename}: {e}")
    
    print(f"\nDone! Files saved to: {output_folder}")

# Test function
def test_with_sample():
    """Test the processor with a sample file"""
    test_file = "path/to/your/inflow_05_09_Alabama.csv"  # CHANGE THIS
    output_folder = "test_output"
    
    Path(output_folder).mkdir(exist_ok=True)
    
    processor = MigrationFlowProcessor(".", output_folder)
    df = processor.process_file(test_file)
    
    if df is not None:
        print("\n" + "="*60)
        print("FIPS CODE VERIFICATION:")
        print("="*60)
        
        # Show FIPS columns
        fips_cols = ['origin_fips_state', 'origin_fips_county', 
                    'dest_fips_state', 'dest_fips_county']
        
        for col in fips_cols:
            if col in df.columns:
                print(f"\n{col}:")
                print(f"  Type: {df[col].dtype}")
                print(f"  Sample values: {df[col].dropna().iloc[:5].tolist()}")
        
        # Show first few rows
        print("\n" + "="*60)
        print("FIRST 5 ROWS:")
        print("="*60)
        print(df[['origin_fips_state', 'origin_fips_county', 
                  'dest_fips_state', 'dest_fips_county',
                  'origin_state_name', 'dest_state_name']].head())
    
    return df

# Main execution
if __name__ == "__main__":
    # SET YOUR PATHS HERE
    input_folder = "migration.data/pre.processed.census.files/outflow_16_20"  # <-- CHANGE THIS
    output_folder = "migration.data/post.processed.census.files/outflow"  # <-- CHANGE THIS
    
    # Option 1: Test with one file first
    # df = test_with_sample()
    
    # Option 2: Process all files with the simple approach
    #simple_process_with_fips(input_folder, output_folder)
    
    # Option 3: Process all files with the full processor
    processor = MigrationFlowProcessor(input_folder, output_folder)
    results = processor.process_all_files()
