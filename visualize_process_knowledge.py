"""
VISUALIZE PROCESS KNOWLEDGE
===========================
Goal: Turn process_knowledge_v1.csv into actionable charts.
1. Top Companies by Data Density (Bar Chart)
2. Top Materials/Chemicals (Tree Map)
"""
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import squarify

# Set Style
sns.set_theme(style="whitegrid")
plt.rcParams['figure.figsize'] = [12, 8]

def visualize():
    try:
        df = pd.read_csv("exports/process_knowledge_v1.csv")
    except FileNotFoundError:
        print("CSV not found.")
        return

    print(f"Loaded {len(df)} extracted insights.")

    # 1. CLEANING
    # Simplify keywords (remove 'Context_' prefix)
    df['Category'] = df['Keyword'].apply(lambda x: x.replace('Context_', '').replace('DATA_', ''))
    
    # 2. TOP COMPANIES (Data Density)
    top_companies = df['Company'].value_counts().head(15)
    
    plt.figure()
    ax = sns.barplot(x=top_companies.values, y=top_companies.index, palette="viridis")
    ax.set_title("Process Intelligence: Top 15 Most Transparent Companies")
    ax.set_xlabel("Number of Extracted Insights (Feedstocks, Wastes, Vols)")
    plt.tight_layout()
    plt.savefig("exports/chart_top_companies.png")
    print("Saved exports/chart_top_companies.png")

    # 3. TOP MATERIALS (Tree Map)
    # Filter for material-related keywords only
    materials = df[~df['Category'].isin(['VOLUME', 'CAS_NUMBER'])]['Category'].value_counts()
    
    plt.figure()
    squarify.plot(sizes=materials.values, label=materials.index, alpha=0.8, color=sns.color_palette("Spectral", len(materials)))
    plt.axis('off')
    plt.title("Dominant Material Flows in Dataset")
    plt.tight_layout()
    plt.savefig("exports/chart_material_treemap.png")
    print("Saved exports/chart_material_treemap.png")

    # 4. CAS NUMBER DISTRIBUTION
    cas_counts = df[df['Keyword'] == 'DATA_CAS_NUMBER']['Context (Excerpt)'].apply(lambda x: x.split('(')[0].strip()).value_counts().head(10)
    
    if not cas_counts.empty:
        plt.figure()
        sns.barplot(x=cas_counts.values, y=cas_counts.index, palette="magma")
        plt.title("Top 10 Identified Chemicals (CAS)")
        plt.tight_layout()
        plt.savefig("exports/chart_top_chemicals.png")
        print("Saved exports/chart_top_chemicals.png")

if __name__ == "__main__":
    visualize()
