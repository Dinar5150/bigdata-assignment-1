"""
performance.py

Task IV: Reads timing results from all 4 databases and creates
a summary table + bar chart visualization.
"""
import json
import matplotlib.pyplot as plt
import numpy as np

DATABASES = ["PostgreSQL", "Citus", "ScyllaDB", "MongoDB"]
FILES = ["results_postgres.json", "results_citus.json", "results_scylladb.json", "results_mongodb.json"]
QUERIES = ["Q1", "Q2", "Q3", "Q4"]

def load_results():
    all_data = {}
    for db, fname in zip(DATABASES, FILES):
        try:
            with open(fname) as f:
                all_data[db] = json.load(f)
        except FileNotFoundError:
            print(f"Warning: {fname} not found, using zeros")
            all_data[db] = {q: 0 for q in QUERIES}
    return all_data

def print_table(data):
    print("\n" + "=" * 70)
    print("Performance Summary Table (avg seconds)")
    print("=" * 70)
    header = f"{'Database':<15}" + "".join(f"{q:<12}" for q in QUERIES)
    print(header)
    print("-" * 70)
    for db in DATABASES:
        row = f"{db:<15}"
        for q in QUERIES:
            val = data[db].get(q, 0)
            row += f"{val:<12.4f}"
        print(row)
    print("=" * 70)

def plot_charts(data):
    x = np.arange(len(QUERIES))
    width = 0.2

    fig, ax = plt.subplots(figsize=(10, 6))
    for i, db in enumerate(DATABASES):
        values = [data[db].get(q, 0) for q in QUERIES]
        ax.bar(x + i * width, values, width, label=db)

    ax.set_xlabel("Query")
    ax.set_ylabel("Avg Execution Time (seconds)")
    ax.set_title("Query Performance Comparison Across Databases")
    ax.set_xticks(x + width * 1.5)
    ax.set_xticklabels(QUERIES)
    ax.legend()
    ax.grid(axis="y", alpha=0.3)

    plt.tight_layout()
    plt.savefig("performance_chart.png", dpi=150)
    print("Chart saved to performance_chart.png")
    plt.show()

    # also make individual charts per query
    fig2, axes = plt.subplots(2, 2, figsize=(12, 8))
    for idx, q in enumerate(QUERIES):
        ax = axes[idx // 2][idx % 2]
        values = [data[db].get(q, 0) for db in DATABASES]
        bars = ax.bar(DATABASES, values, color=["#4C72B0", "#DD8452", "#55A868", "#C44E52"])
        ax.set_title(f"{q} Execution Time")
        ax.set_ylabel("Seconds")
        for bar, val in zip(bars, values):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height(),
                    f"{val:.3f}", ha="center", va="bottom", fontsize=8)

    plt.tight_layout()
    plt.savefig("performance_per_query.png", dpi=150)
    print("Per-query chart saved to performance_per_query.png")
    plt.show()

if __name__ == "__main__":
    data = load_results()
    print_table(data)
    plot_charts(data)
