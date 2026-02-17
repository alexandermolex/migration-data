import pandas as pd
import glob
import os
from pathlib import Path
import re

class MigrationFlowProcessor:
    """
    Custom processor for Census Bureau migration flow CSV files
    Properly pairs estimate and MOE columns with descriptive names
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
            df = pd.read_csv(file_path, header=None, skiprows=4, dtype=str)
            
            # Ensure we have the right number of columns
            if len(df.columns) > len(column_names):
                df = df.iloc[:, :len(column_names)]
            elif len(df.columns) < len(column_names):
                for i in range(len(df.columns), len(column_names)):
                    df[i] = pd.NA
            
            # Assign column names
            df.columns = column_names
            
            # Convert numeric columns
            for col in df.columns:
                if col.endswith('_estimate') or col.endswith('_moe') or 'population' in col:
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


# Even simpler approach - just let pandas handle it
def simple_process(input_folder, output_folder):
    """
    Simple processor that reads files with pandas default behavior
    and adds consistent metadata
    """
    Path(output_folder).mkdir(parents=True, exist_ok=True)
    
    csv_files = glob.glob(os.path.join(input_folder, "*.csv"))
    print(f"Found {len(csv_files)} files")
    
    for i, file_path in enumerate(csv_files, 1):
        filename = os.path.basename(file_path)
        print(f"\rProcessing {i}/{len(csv_files)}: {filename}", end="")
        
        try:
            # Read with pandas default header detection
            df = pd.read_csv(file_path, skiprows=4)
            
            # Clean up column names (remove extra spaces, lowercase)
            df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
            
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
    test_file = "migration.data/pre.processed.census.files/inflow_05_09/inflow_05_09_Alabama.csv"  # CHANGE THIS
    output_folder = "test_output"
    
    Path(output_folder).mkdir(exist_ok=True)
    
    processor = MigrationFlowProcessor(".", output_folder)
    df = processor.process_file(test_file)
    
    if df is not None:
        print("\n" + "="*60)
        print("COLUMN NAMES (first 20):")
        print("="*60)
        for i, col in enumerate(df.columns[:20]):
            print(f"{i:2d}. {col}")
        
        # Show estimate/MOE pairs
        print("\n" + "="*60)
        print("ESTIMATE/MOE PAIRS:")
        print("="*60)
        estimate_cols = [col for col in df.columns if col.endswith('_estimate')]
        for est in estimate_cols[:10]:  # Show first 10 pairs
            moe = est.replace('_estimate', '_moe')
            if moe in df.columns:
                print(f"  {est}")
                print(f"  {moe}")
                print()
    
    return df

# Main execution
if __name__ == "__main__":
    # SET YOUR PATHS HERE
    input_folder = "migration.data/pre.processed.census.files/outflow_16_20"  # <-- CHANGE THIS
    output_folder = "migration.data/post.processed.census.files/outflow"  # <-- CHANGE THIS
    
    # Option 1: Test with one file first
    #df = test_with_sample()
    
    # Option 2: Process all files
    processor = MigrationFlowProcessor(input_folder, output_folder)
    results = processor.process_all_files()