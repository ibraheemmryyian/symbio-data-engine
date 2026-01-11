"""
AUDIT PDF SCOPE
===============
Goal: Analyze the quality of the 425 existing CSR reports.
Metrics: Keyword density for "Process Knowledge" (Feedstock, Waste, Emissions).
"""
import os
import random
from pathlib import Path
from collections import Counter
import pypdf

def audit_pdfs(directory="data/raw/csr_reports", sample_size=50):
    dir_path = Path(directory)
    files = list(dir_path.glob("*.pdf"))
    
    if not files:
        print("No PDFs found.")
        return

    # Sample if too many
    if len(files) > sample_size:
        print(f"Sampling {sample_size} out of {len(files)} files...")
        selection = random.sample(files, sample_size)
    else:
        selection = files

    keywords = {
        "Process Knowledge": ["feedstock", "raw material", "input material", "manufacturing process"],
        "Waste Streams": ["hazardous waste", "by-product", "effluent", "scrap", "tailings", "sludge"],
        "Chemicals": ["chemical composition", "cas number", "substance", "reagent"],
        "Quantitative": ["tonnes", "metric tons", "kg/year", "tpa"]
    }
    
    results = {k: 0 for k in keywords}
    file_scores = []

    print(f"\nScanning {len(selection)} PDFs...")
    
    for pdf_path in selection:
        try:
            text = ""
            with open(pdf_path, 'rb') as f:
                reader = pypdf.PdfReader(f)
                # Read first 50 pages max to speed up
                for page in reader.pages[:50]:
                    extracted = page.extract_text()
                    if extracted: text += extracted.lower()
            
            score = 0
            hits = []
            for category, terms in keywords.items():
                if any(term in text for term in terms):
                    results[category] += 1
                    score += 1
                    hits.append(category)
            
            file_scores.append((pdf_path.name, score, hits))
            print(f".", end="", flush=True)
            
        except Exception as e:
            print(f"x", end="", flush=True)

    print("\n\n=== PDF AUDIT REPORT ===")
    print(f"Total Audited: {len(selection)}")
    print("\nCategory Coverage (Percentage of files containing terms):")
    for cat, count in results.items():
        pct = (count / len(selection)) * 100
        print(f"  - {cat}: {pct:.1f}%")

    print("\nTop High-Value Files (Process Rich):")
    # Sort by score (number of categories matched)
    top_files = sorted(file_scores, key=lambda x: x[1], reverse=True)[:10]
    for name, score, hits in top_files:
        print(f"  [{score}/4] {name[:50]}... -> {hits}")

if __name__ == "__main__":
    audit_pdfs()
