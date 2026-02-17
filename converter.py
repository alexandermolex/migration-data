import tkinter as tk
from tkinter import filedialog, messagebox
import pandas as pd
import os
from pathlib import Path

def select_files_to_convert(prompt="Select files to convert"):
    """Allow selection of multiple files at once"""
    root = tk.Tk()
    root.withdraw()
    
    # Allow multiple file selection
    filepaths = filedialog.askopenfilenames(
        filetypes=[("CSV files", "*.csv"), ("Excel files", "*.xls;*.xlsx")], 
        title=prompt
    )
    
    return filepaths if filepaths else None

def convert_excel_sheets_to_csv(excel_path, output_dir=None, base_name=None):
    """
    Convert all sheets in an Excel file to separate CSV files
    
    Args:
        excel_path: Path to Excel file
        output_dir: Directory to save CSV files (default: same as Excel file)
        base_name: Base name for output files (default: original filename)
    
    Returns:
        List of generated CSV file paths
    """
    excel_path = Path(excel_path)
    
    # Set output directory
    if output_dir is None:
        output_dir = excel_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    # Set base name for output files
    if base_name is None:
        base_name = excel_path.stem
    
    try:
        # Read all sheet names first
        excel_file = pd.ExcelFile(excel_path)
        sheet_names = excel_file.sheet_names
        
        if not sheet_names:
            messagebox.showwarning("Empty Excel File", f"The file {excel_path.name} contains no sheets.")
            return []
        
        generated_files = []
        
        # Process each sheet
        for i, sheet_name in enumerate(sheet_names, 1):
            try:
                # Read the sheet
                df = pd.read_excel(excel_path, sheet_name=sheet_name)
                
                # Clean sheet name for filename (remove invalid characters)
                clean_sheet = "".join(c for c in sheet_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
                if not clean_sheet:  # If sheet name becomes empty after cleaning
                    clean_sheet = f"Sheet{i}"
                
                # Create output filename
                output_file = output_dir / f"{base_name}_{clean_sheet}.csv"
                
                # Save to CSV
                df.to_csv(output_file, index=False, encoding='utf-8-sig')
                
                generated_files.append({
                    'path': str(output_file),
                    'sheet': sheet_name,
                    'rows': len(df),
                    'columns': len(df.columns)
                })
                
                print(f"  ✓ Sheet '{sheet_name}': {len(df)} rows, {len(df.columns)} columns -> {output_file.name}")
                
            except Exception as e:
                print(f"  ✗ Error processing sheet '{sheet_name}': {str(e)}")
                continue
        
        return generated_files
        
    except Exception as e:
        messagebox.showerror("Error Reading Excel", f"Could not read {excel_path.name}: {str(e)}")
        return []

def convert_csv_to_csv(csv_path, output_dir=None, base_name=None):
    """
    Convert/process a CSV file (just copies/renames it)
    """
    csv_path = Path(csv_path)
    
    if output_dir is None:
        output_dir = csv_path.parent
    else:
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)
    
    if base_name is None:
        base_name = csv_path.stem
    
    try:
        # Read the CSV
        df = pd.read_csv(csv_path)
        
        # Create output filename
        output_file = output_dir / f"{base_name}.csv"
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8-sig')
        
        print(f"  ✓ Processed CSV: {len(df)} rows, {len(df.columns)} columns -> {output_file.name}")
        
        return [{
            'path': str(output_file),
            'sheet': 'CSV',
            'rows': len(df),
            'columns': len(df.columns)
        }]
        
    except Exception as e:
        messagebox.showerror("Error Reading CSV", f"Could not read {csv_path.name}: {str(e)}")
        return []

def process_files():
    """Main function to process selected files"""
    
    # Ask user how they want to process files
    root = tk.Tk()
    root.withdraw()
    
    # Get conversion mode
    mode = messagebox.askyesno(
        "Conversion Mode", 
        "Do you want to convert multiple files at once?\n\n"
        "Yes: Select multiple files now\n"
        "No: Specify number of files to select one by one"
    )
    
    all_files = []
    
    if mode:  # Multiple files at once
        files = select_files_to_convert("Select files to convert (Ctrl+Click for multiple)")
        if files:
            all_files = list(files)
            print(f"\nSelected {len(all_files)} files for conversion")
    else:  # One by one
        num_of_files = int(input("\nHow many files do you want to convert? "))
        print(f"Select {num_of_files} files one by one...")
        
        for i in range(num_of_files):
            file = select_files_to_convert(f"Select file {i+1} of {num_of_files}")
            if file and len(file) > 0:
                all_files.append(file[0])  # Take first file from tuple
            else:
                print(f"  ✗ File {i+1} selection cancelled")
    
    if not all_files:
        print("\nNo files selected. Exiting.")
        return
    
    # Ask for output directory
    use_custom_dir = messagebox.askyesno(
        "Output Directory",
        "Do you want to save files to a custom directory?\n\n"
        "Yes: Choose output directory\n"
        "No: Save in same location as input files"
    )
    
    custom_output_dir = None
    if use_custom_dir:
        custom_output_dir = filedialog.askdirectory(title="Select Output Directory")
        if not custom_output_dir:
            print("\nNo output directory selected. Using input file locations.")
            custom_output_dir = None
    
    # Ask for base name prefix
    use_custom_name = messagebox.askyesno(
        "File Naming",
        "Do you want to specify a base name for output files?\n\n"
        "Yes: Enter a base name\n"
        "No: Use original filenames"
    )
    
    custom_base_name = None
    if use_custom_name:
        custom_base_name = input("Enter base name for output files: ").strip()
        if not custom_base_name:
            custom_base_name = None
    
    # Process each file
    print("\n" + "="*50)
    print("STARTING CONVERSION...")
    print("="*50)
    
    all_results = []
    
    for file_path in all_files:
        file_path = Path(file_path)
        ext = file_path.suffix.lower()
        
        print(f"\n📁 Processing: {file_path.name}")
        
        # Determine output directory for this file
        if custom_output_dir:
            output_dir = custom_output_dir
        else:
            output_dir = file_path.parent
        
        # Determine base name for this file
        if custom_base_name:
            # If multiple files, add index to base name
            if len(all_files) > 1:
                file_index = all_files.index(str(file_path)) + 1
                base_name = f"{custom_base_name}_{file_index:02d}"
            else:
                base_name = custom_base_name
        else:
            base_name = file_path.stem
        
        # Process based on file type
        if ext in ['.xls', '.xlsx']:
            results = convert_excel_sheets_to_csv(file_path, output_dir, base_name)
            all_results.extend(results)
            
        elif ext == '.csv':
            results = convert_csv_to_csv(file_path, output_dir, base_name)
            all_results.extend(results)
    
    # Print summary
    print("\n" + "="*50)
    print("CONVERSION SUMMARY")
    print("="*50)
    print(f"Total files processed: {len(all_files)}")
    
    excel_files = [f for f in all_files if Path(f).suffix.lower() in ['.xls', '.xlsx']]
    csv_files = [f for f in all_files if Path(f).suffix.lower() == '.csv']
    
    if excel_files:
        print(f"Excel files: {len(excel_files)}")
    if csv_files:
        print(f"CSV files: {len(csv_files)}")
    
    if all_results:
        print(f"\nTotal CSV files generated: {len(all_results)}")
        print("\nGenerated files:")
        for i, result in enumerate(all_results, 1):
            print(f"  {i}. {Path(result['path']).name} ({result['rows']} rows)")
        
        # Show where files were saved
        save_location = Path(all_results[0]['path']).parent
        print(f"\n💾 Files saved in: {save_location}")
    
    print("\n" + "="*50)

if __name__ == "__main__":
    try:
        process_files()
        print("\n✨ Conversion complete!")
    except Exception as e:
        print(f"\n❌ An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    
    input("\nPress Enter to exit...")