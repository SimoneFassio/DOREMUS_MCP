import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors

ORIGIN_DATA_DIR = "experiments"
GPT_FILENAME = "Doremus_Questions_1.1_PC_30s-timeout-gpt-4.1-1c61477b.json"
QWEN30B_FILENAME = "Doremus_Questions_1.1_D4-qwen3-coder-30b-04b611b3.json"
QWEN480B_FILENAME = "Doremus_Questions_1.1_PC_30s-timeout-qwen-qwen3-coder-480b-a35b-instruct-00ae6bb5.json"
QWEN30B_BQ_AF_FILENAME = "Doremus_Questions_1.1_Config_2_BQ_AF-qwen3-coder-30b-bb5f8234.json"
QWEN30B_BQ_AF_FBQ_FILENAME = "Doremus_Questions_1.1_Config_3_BQ_AF_FBQ-qwen3-coder-30b-19dfbf62.json"
QWEN30B_BQ_AF_FBQ_ACC_FILENAME = "Doremus_Questions_1.1_Config_4_BQ_AF_FBQ_ACC-qwen3-coder-30b-4b26a45c.json"
QWEN30B_BQ_AF_FBQ_ACC_SAV_FILENAME = "Doremus_Questions_1.1_Config_5_BQ_AF_FBQ_ACC_SAV-qwen3-coder-30b-d9d0488a.json"
QWEN30B_FULL_GH_FILENAME = "Doremus_Questions_1.1_Config_6_FULL_GH-qwen3-coder-30b-8b9b4fc4.json"
PLOTS_DIR_OUTPUT = "data/evaluation/plots/"


def create_scatter_accuracy_consistency(data):
    """
    Create a scatter plot of accuracy vs consistency.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and 'consistency' keys.
    """
    accuracies = [item['accuracy'] for item in data]
    consistencies = [item['consistency'] for item in data]
    labels = [item.get('label', '') for item in data]

    plt.figure(figsize=(6, 5))
    style = {
        "GPT-4.1": ("tab:blue", "s"),
        "QWEN-3 Coders 30B": ("tab:orange", "o"),
        "QWEN-3 Coders 480B": ("tab:green", "^"),
    }
    default_color, default_marker = ("tab:gray", "D")

    seen = set()
    for i, label in enumerate(labels):
        color, marker = style.get(label, (default_color, default_marker))

        # only add legend label once per model
        legend_label = label if label not in seen else None
        seen.add(label)

        plt.scatter(
            accuracies[i] * 100,
            consistencies[i] * 100,
            s=200,
            color=color,
            marker=marker,
            label=legend_label,
        )

    plt.xlabel('Accuracy (%)')
    plt.ylabel('Consistency (%)')
    plt.title('Scatter Plot of Accuracy vs Consistency')
    plt.xlim(20, 60)
    plt.ylim(60, 104)
    plt.grid(alpha=0.3)
    plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + 'accuracy_vs_consistency.png')

def create_scatter_accuracy_vs_token_cost(data):
    """
    Create a scatter plot of accuracy vs total token cost.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and 'total_token_cost' keys.
    """
    accuracies = [item['accuracy'] for item in data]
    token_costs = [item['total_token_cost'] for item in data]
    labels = [item.get('label', '') for item in data]

    plt.figure(figsize=(6, 5))
    style = {
        "GPT-4.1": ("tab:blue", "s"),
        "QWEN-3 Coders 30B": ("tab:orange", "o"),
        "QWEN-3 Coders 480B": ("tab:green", "^"),
    }
    default_color, default_marker = ("tab:gray", "D")

    seen = set()
    for i, label in enumerate(labels):
        color, marker = style.get(label, (default_color, default_marker))

        # only add legend label once per model
        legend_label = label if label not in seen else None
        seen.add(label)

        plt.scatter(
            accuracies[i] * 100,
            token_costs[i] / 1e6,
            s=200,
            color=color,
            marker=marker,
            label=legend_label,
        )
    plt.xlabel('Accuracy')
    plt.ylabel('Total Token Cost of run (Millions)')
    plt.title('Scatter Plot of Accuracy vs Total Token Cost')
    plt.xlim(20, 60)
    plt.ylim(4, 7)
    plt.grid(alpha=0.3)
    plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + 'accuracy_vs_token_cost.png')

def create_scatter_accuracy_vs_latency(data):
    """
    Create a scatter plot of accuracy vs average latency.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and 'average_latency' keys.
    """
    accuracies = [item['accuracy'] for item in data]
    latencies = [item['average_latency'] for item in data]
    labels = [item.get('label', '') for item in data]

    plt.figure(figsize=(6, 5))
    style = {
        "GPT-4.1": ("tab:blue", "s"),
        "QWEN-3 Coders 30B": ("tab:orange", "o"),
        "QWEN-3 Coders 480B": ("tab:green", "^"),
    }
    default_color, default_marker = ("tab:gray", "D")

    seen = set()
    for i, label in enumerate(labels):
        color, marker = style.get(label, (default_color, default_marker))

        # only add legend label once per model
        legend_label = label if label not in seen else None
        seen.add(label)

        plt.scatter(
            accuracies[i] * 100,
            latencies[i],
            s=200,
            color=color,
            marker=marker,
            label=legend_label,
        )
    plt.xlabel('Accuracy')
    plt.ylabel('Average Latency (seconds)')
    plt.title('Scatter Plot of Accuracy vs Average Latency')
    plt.xlim(20, 60)
    plt.ylim(50, 130)
    plt.grid(alpha=0.3)
    plt.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + 'accuracy_vs_latency.png')

def create_heatmap_by_complexity(models_data, model_name):
    """
    Create a heatmap of accuracy by question complexity for different models.

    Args:
        models_data (list of dict): List containing dictionaries with model names and accuracies by complexity.
    """
    complexities = ['easy', 'medium', 'hard']
    accuracy_matrix = np.array([[item.get(f"{complexity}_accuracy", np.nan) for complexity in complexities] for item in models_data])

    plt.figure(figsize=(7, 5))
    cmap = plt.cm.get_cmap("RdYlGn")
    norm = mcolors.Normalize(vmin=0.0, vmax=1.0)  # accuracies are fractions 0..1

    im = plt.imshow(accuracy_matrix, cmap=cmap, norm=norm, aspect="auto")

    for i in range(accuracy_matrix.shape[0]):
        for j in range(accuracy_matrix.shape[1]):
            val = accuracy_matrix[i, j]
            if np.isnan(val):
                continue

            r, g, b, _ = cmap(norm(val))
            # perceived luminance (0=dark, 1=bright)
            luminance = 0.2126 * r + 0.7152 * g + 0.0722 * b
            text_color = "black" if luminance > 0.6 else "white"

            plt.text(
                j, i, f"{val * 100:.1f}%",
                ha="center", va="center",
                color=text_color
            )

    plt.colorbar(im, label="Accuracy")
    plt.xticks(ticks=np.arange(len(complexities)), labels=complexities)
    plt.yticks(ticks=np.arange(len(models_data)), labels=[item['label'] for item in models_data])
    plt.xlabel('Question Complexity')
    plt.ylabel('Models')
    plt.title('Heatmap of Accuracy by Question Complexity')
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + 'accuracy_heatmap_by_complexity.png')

def create_stacked_bar_failure_types(models_data):
    """
    Create a stacked bar chart of failure types for different models.

    Args:
        models_data (list of dict): List containing dictionaries with model names and failure type rates.
    """
    labels = [item['label'] for item in models_data]
    type_1_errors = [item.get('type_1_error_rate', 0) for item in models_data]
    type_2_errors = [item.get('type_2_error_rate', 0) for item in models_data]
    type_3_errors = [item.get('type_3_error_rate', 0) for item in models_data]

    x = np.arange(len(labels))
    width = 0.35

    plt.figure(figsize=(10, 6))
    plt.bar(x, type_1_errors, width, label='Type 1 Errors', color='steelblue')
    plt.bar(x, type_2_errors, width, bottom=type_1_errors, label='Type 2 Errors', color='mediumaquamarine')
    bottom_type_3 = np.array(type_1_errors) + np.array(type_2_errors)
    plt.bar(x, type_3_errors, width, bottom=bottom_type_3, label='Type 3 Errors', color='mediumpurple')

    plt.xlabel('Models')
    plt.ylabel('Avg error occurrence per run')
    plt.title('Stacked Bar Chart of Failure Types by Model')
    plt.xticks(ticks=x, labels=labels)
    plt.legend(title='Failure Types', loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + 'failure_types_stacked_bar.png')

def line_chart_config_accuracy(data, model_name):
    """
    Create a line chart of accuracy across different configurations for a model.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and configuration names.
    """
    configurations = [item['label'] for item in data]
    accuracies = [item['accuracy'] for item in data]

    plt.figure(figsize=(8, 5))
    plt.plot(configurations, [acc * 100 for acc in accuracies], linestyle='--')
    plt.scatter(configurations, [acc * 100 for acc in accuracies], s=100, marker='s')
    for i, acc in enumerate(accuracies):
        plt.text(i, acc * 100 + 3, f"{acc * 100:.1f}%", ha='center', color='steelblue')
    plt.xlabel('Configurations')
    plt.ylabel('Accuracy (%)')
    plt.title(f'Line Chart of Accuracy across Configurations for {model_name}')
    plt.ylim(10, 60)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + f'{model_name}_config_accuracy_line_chart.png')

def clean_data_for_plotting(runs_data, model_name):
    """
    Clean and structure the runs data for plotting.

    Args:
        runs_data (JSON): JSON data containing runs information.
    Returns:
        dict: Cleaned data structured for plotting.
    """
    cleaned_data = {"label": model_name}
    data = runs_data.copy()
    # Average accuracy obtained across all questions
    cleaned_data['accuracy'] = data['metrics'].apply(lambda x: x.get('accuracy', np.nan)).mean()
    # Mean accuracy by difficulty (row filtering via .loc)
    if "difficulty" in data.columns:
        for diff in ("easy", "medium", "hard"):
            subset = data.loc[data["difficulty"] == diff, "metrics"].apply(
                lambda d: d.get("accuracy", np.nan) if isinstance(d, dict) else np.nan
            )
            cleaned_data[f"{diff}_accuracy"] = float(subset.mean()) if len(subset) else np.nan
    else:
        cleaned_data["easy_accuracy"] = np.nan
        cleaned_data["medium_accuracy"] = np.nan
        cleaned_data["hard_accuracy"] = np.nan
    # Consistency: percentage of equal accuracy measurements for each question
    data["accuracy"] = data["metrics"].apply(
        lambda d: d.get("accuracy", np.nan) if isinstance(d, dict) else np.nan
    )
    # per-question variation
    per_q_std = data.groupby("question")["accuracy"].std(ddof=0)
    cleaned_data["mean_question_std"] = float(per_q_std.mean(skipna=True))
    cleaned_data["consistency"] = float(np.clip(1.0 - 2.0 * cleaned_data["mean_question_std"], 0.0, 1.0))
    # Total token cost
    n_grouped_runs = data.groupby("question").size().iloc[0]
    cleaned_data["total_token_cost"] = data["metrics"].apply(lambda x: x.get("tokens", np.nan) if isinstance(x, dict) else np.nan).sum()/n_grouped_runs
    # Average latency
    cleaned_data["average_latency"] = data["metrics"].apply(lambda x: x.get("latency", np.nan) if isinstance(x, dict) else np.nan).mean()
    # Failure type I
    cleaned_data["type_1_error_rate"] = data["metrics"].apply(lambda x: x.get("type 1 errors", np.nan) if isinstance(x, dict) else np.nan).mean()
    # Failure type II
    cleaned_data["type_2_error_rate"] = data["metrics"].apply(lambda x: x.get("type 2 errors", np.nan) if isinstance(x, dict) else np.nan).mean()
    # Failure type III
    cleaned_data["type_3_error_rate"] = data["metrics"].apply(lambda x: x.get("type 3 errors", np.nan) if isinstance(x, dict) else np.nan).mean()

    output=f"\
\nModel: {model_name}\n\
    ---------------ACCURACY METRICS---------------\n\
    Overall Accuracy: {cleaned_data['accuracy']}\n\
    Easy Accuracy: {cleaned_data['easy_accuracy']}\n\
    Medium Accuracy: {cleaned_data['medium_accuracy']}\n\
    Hard Accuracy: {cleaned_data['hard_accuracy']}\n\
    Consistency: {cleaned_data['consistency']}\n\
    ---------------COST METRICS---------------\n\
    Total Token Cost: {cleaned_data['total_token_cost']}\n\
    Average Latency: {cleaned_data['average_latency']} seconds\n\
    ---------------ERROR METRICS---------------\n\
    Type 1 Error Rate: {cleaned_data['type_1_error_rate']}\n\
    Type 2 Error Rate: {cleaned_data['type_2_error_rate']}\n\
    Type 3 Error Rate: {cleaned_data['type_3_error_rate']}\n\
"
    print(output)
    return cleaned_data
    


if __name__ == "__main__":
    gpt_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT_FILENAME))
    qwen30b_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_FILENAME))
    qwen480b_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN480B_FILENAME))

    qwen30b_bq_af_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FILENAME))
    qwen30b_bq_af_fbq_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_FILENAME))
    qwen30b_bq_af_fbq_acc_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_ACC_FILENAME))
    qwen30b_bq_af_fbq_acc_sav_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_ACC_SAV_FILENAME))
    qwen30b_full_gh_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_FULL_GH_FILENAME))

    # TEST ON DIFFERENT LLMS
    data_cleaned = []
    data_cleaned.append(clean_data_for_plotting(qwen30b_data, "QWEN-3 Coders 30B"))
    data_cleaned.append(clean_data_for_plotting(gpt_data, "GPT-4.1"))
    data_cleaned.append(clean_data_for_plotting(qwen480b_data, "QWEN-3 Coders 480B"))
    create_scatter_accuracy_consistency(data_cleaned)
    create_scatter_accuracy_vs_token_cost(data_cleaned)
    create_scatter_accuracy_vs_latency(data_cleaned)
    create_heatmap_by_complexity(data_cleaned, "Models Comparison by Question Complexity")
    create_stacked_bar_failure_types(data_cleaned)

    # TEST ON DIFFERENT CONFIGURATIONS OF THE SAME MODEL
    data_cleaned_configs = []
    data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_data, "BQ + AF"))
    data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_data, "+ FBQ"))
    data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_acc_data, "+ ACC"))
    data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_acc_sav_data, "+ SAV"))
    data_cleaned_configs.append(clean_data_for_plotting(qwen30b_full_gh_data, "+ GH"))
    line_chart_config_accuracy(data_cleaned_configs, "QWEN-3 Coders 30B")
