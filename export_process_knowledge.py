"""
EXPORT PROCESS KNOWLEDGE
========================
Goal: Extract structured "Process Knowledge" from the harvested CSR reports.
Output: process_knowledge_v1.csv
Technique: Keyword-based sentence extraction from PDF text.
"""
import csv
import re
from pathlib import Path
import pypdf

def clean_text(text):
    return re.sub(r'\s+', ' ', text).strip()

def export_data(directory="data/raw/csr_reports"):
    output_file = "exports/process_knowledge_v1.csv"
    Path("exports").mkdir(exist_ok=True)
    
    files = list(Path(directory).glob("*.pdf"))
    if not files:
        print("No PDFs found.")
        return

    print(f"Extracting knowledge from {len(files)} reports...")
    
    # 1. Chemical/Process Keywords
    keywords = [
        "feedstock", "raw material", "by-product", "waste stream", 
        "effluent", "tailings", "hazardous waste", "input material",
        "chemical composition", "reagent", "catalyst", "solvent"
    ]
    
    # 2. Open CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Company", "Filename", "Keyword", "Context (Excerpt)", "Page"])
        
        count = 0
        
        for i, pdf_path in enumerate(files):
            try:
                company = pdf_path.name.split('_')[0]
                
                with open(pdf_path, 'rb') as f:
                    try:
                        reader = pypdf.PdfReader(f)
                        num_pages = len(reader.pages)
                        # Limit to first 30 pages + random 10 middle for speed/relevance
                        pages_to_scan = range(min(num_pages, 40))
                        
                        for page_num in pages_to_scan:
                            page = reader.pages[page_num]
                            text = page.extract_text()
                            if not text: continue
                            
                            lower_text = text.lower()
                            
                            for kw in keywords:
                                if kw in lower_text:
                                    # Extract sentence/context around the keyword
                                    for match in re.finditer(re.escape(kw), lower_text):
                                        start = max(0, match.start() - 100)
                                        end = min(len(text), match.end() + 100)
                                        excerpt = clean_text(text[start:end])
                                        writer.writerow([company, pdf_path.name, "Context_" + kw, excerpt, page_num])
                                        count += 1
                            
                            # LEVEL 2: QUANTITATIVE EXTRACTION
                            # 1. CAS Numbers (Chemical Abstracts Service)
                            cas_matches = re.finditer(r'\b[1-9]\d{1,5}-\d{2}-\d\b', text)
                            for m in cas_matches:
                                context_start = max(0, m.start() - 30)
                                context_end = min(len(text), m.end() + 30)
                                writer.writerow([
                                    company, pdf_path.name, 
                                    "DATA_CAS_NUMBER", 
                                    f"{m.group(0)} ({clean_text(text[context_start:context_end])})", 
                                    page_num
                                ])
                                count += 1

                            # 2. Volumes (Tonnes/Litres)
                            # Pattern: Number + space + Unit (tonnes, tpa, mt, kg)
                            vol_matches = re.finditer(r'\b(\d{1,3}(,\d{3})*(\.\d+)?)\s+(tonnes|tpa|metric tons|kg|kilograms)\b', lower_text)
                            for m in vol_matches:
                                context_start = max(0, m.start() - 50)
                                context_end = min(len(text), m.end() + 50)
                                writer.writerow([
                                    company, pdf_path.name, 
                                    "DATA_VOLUME", 
                                    f"{m.group(0)} | Context: {clean_text(text[context_start:context_end])}", 
                                    page_num
                                ])
                                count += 1
                                        
                    except Exception as e:
                        print(f"Error reading PDF {pdf_path.name}: {e}")
            except Exception as e:
                pass
            
            if i % 10 == 0:
                print(f"Processed {i}/{len(files)} files. Extracted {count} insights.")

    print(f"\nSUCCESS: Exported {count} process insights to {output_file}")

if __name__ == "__main__":
    export_data()
