import os
import io
import textwrap
import cairosvg
from PIL import Image
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
from matplotlib.offsetbox import OffsetImage, AnnotationBbox

ORIGIN_DATA_DIR = "experiments"
GPT4_1_FILENAME = "Doremus_Questions_1.1_PC_30s-timeout-gpt-4.1-1c61477b.json"
GPT5_2_FILENAME = "Doremus_Questions_1.1_D4-gpt-5.2-f875f349.json"
QWEN30B_FILENAME = "Doremus_Questions_1.1_PC_30s-timeout-qwen3-coder-30b-129c9985.json"
QWEN480B_FILENAME = "Doremus_Questions_1.1_PC_30s-timeout-qwen-qwen3-coder-480b-a35b-instruct-00ae6bb5.json"
QWEN30B_BQ_AF_FILENAME = "Doremus_Questions_1.1_Config_2_BQ_AF-qwen3-coder-30b-bb5f8234.json"
QWEN30B_BQ_AF_FBQ_FILENAME = "Doremus_Questions_1.1_Config_3_BQ_AF_FBQ-qwen3-coder-30b-19dfbf62.json"
QWEN30B_BQ_AF_FBQ_ACC_FILENAME = "Doremus_Questions_1.1_Config_4_BQ_AF_FBQ_ACC-qwen3-coder-30b-4b26a45c.json"
QWEN30B_BQ_AF_FBQ_ACC_SAV_FILENAME = "Doremus_Questions_1.1_Config_5_BQ_AF_FBQ_ACC_SAV-qwen3-coder-30b-d9d0488a.json"
QWEN30B_BQ_AF_FBQ_ACC_SAV_GH_FILENAME = "Doremus_Questions_1.1_Config_6_FULL_GH-qwen3-coder-30b-8b9b4fc4.json"
GPT4_1_WIKIDATA_FILENAME = "Doremus_Questions_1.1_WIKIDATA-gpt-4.1-bf3935e3.json"
GPT4_1_BQ_AF_FILENAME = "Doremus_Questions_1.1_Config_2_BQ_AF-gpt-4.1-bca6fcb6.json"
GPT4_1_BQ_AF_FBQ_FILENAME = "Doremus_Questions_1.1_Config_3_BQ_AF_FBQ-gpt-4.1-3ee40e4f.json"
GPT4_1_BQ_AF_FBQ_ACC_FILENAME = "Doremus_Questions_1.1_Config_4_BQ_AF_FBQ_ACC-gpt-4.1-a86e6cf0.json"
GPT4_1_BQ_AF_FBQ_ACC_SAV_FILENAME = "Doremus_Questions_1.1_Config_5_BQ_AF_FBQ_ACC_SAV-gpt-4.1-2eb8d146.json"
GPT4_1_BQ_AF_FBQ_ACC_SAV_GH_FILENAME = "Doremus_Questions_1.1_Config_6_BQ_AF_FBQ_ACC_SAV_GH-gpt-4.1-8f510930.json"
GPT4_1_SAMPLINGOFF_DRYRUNOFF_FILENAME = "Doremus_Questions_1.1_Sampling_OFF_DryRun_OFF-gpt-4.1-b8a6bf09.json"
GPT4_1_SAMPLINGOFF_DRYRUNON_FILENAME = "Doremus_Questions_1.1_Sampling_OFF_DryRun_ON-gpt-4.1-74359adc.json"
GPT4_1_SAMPLINGON_DRYRUNOFF_FILENAME = "Doremus_Questions_1.1_Sampling_ON_DryRun_OFF-gpt-4.1-3a3b4bb5.json"
GPT4_1_SAMPLINGON_DRYRUNON_FILENAME = "Doremus_Questions_1.1_FULL-gpt-4.1-7d383e36.json"


PLOTS_DIR_OUTPUT = "data/evaluation/plots/"

# STYLE ICONS
style = {
    "GPT-4.1": {"color": "tab:blue", "icon": "data/icons/chatgpt-icon.svg"},
    "GPT-5.2": {"color": "tab:red", "icon": "data/icons/chatgpt-icon.svg"},
    "QWEN-3 Coders 30B": {"color": "tab:orange", "icon": "data/icons/qwen-ai-icon.svg"},
    "QWEN-3 Coders 480B": {"color": "tab:green", "icon": "data/icons/qwen-ai-icon.svg"},
    "GLM-4.7": {"color": "tab:purple", "icon": "data/icons/z-ai-icon.png"},
}
default_style = {"color": "tab:gray", "icon": None}

_ICON_CACHE = {}

def _load_icon_rgba(path: str):
    if path in _ICON_CACHE:
        return _ICON_CACHE[path]

    ext = os.path.splitext(path)[1].lower()

    if ext == ".svg":
        # Optional dependency; only needed for SVG icons.

        png_bytes = cairosvg.svg2png(url=path, output_width=128, output_height=128)
        rgba = np.asarray(Image.open(io.BytesIO(png_bytes)).convert("RGBA"))
    else:
        # PNG recommended if you want zero extra deps.
        rgba = plt.imread(path)

    _ICON_CACHE[path] = rgba
    return rgba

def scatter_icons(
    ax,
    xs,
    ys,
    labels,
    style_map,
    zoom=0.22,
    show_model_name=True,
    name_offset_points=(0, -18),
    name_wrap_width=8,
    name_kwargs=None,
):
    # Keep limits/autoscale consistent (invisible anchors)
    ax.scatter(xs, ys, s=0)

    if name_kwargs is None:
        name_kwargs = {}

    seen = set()
    for x, y, label in zip(xs, ys, labels):
        st = style_map.get(label, {"color": "tab:gray", "icon": None})
        icon_path = st.get("icon")

        if icon_path and os.path.exists(icon_path):
            rgba = _load_icon_rgba(icon_path)
            ab = AnnotationBbox(OffsetImage(rgba, zoom=zoom), (x, y), frameon=False)
            ax.add_artist(ab)
        else:
            ax.scatter([x], [y], s=200, color=st.get("color", "tab:gray"), marker="o")

        # Draw model name under the icon (in display-point offset)
        if show_model_name and label:
            display_label = (
                textwrap.fill(
                    label,
                    width=name_wrap_width,
                    break_long_words=True,
                    break_on_hyphens=True,
                )
                if name_wrap_width
                else label
            )

            ax.annotate(
                display_label,
                (x, y),
                xytext=name_offset_points,
                textcoords="offset points",
                ha="center",
                va="top",
                fontsize=name_kwargs.get("fontsize", 9),
                fontweight=name_kwargs.get("fontweight", "normal"),
                linespacing=name_kwargs.get("linespacing", 1.0),
            )

        # Legend label once per model (still text-only)
        if label not in seen:
            seen.add(label)
            ax.scatter([], [], s=200, color=st.get("color", "tab:gray"), marker="o", label=label)

def create_scatter_accuracy_consistency(data):
    """
    Create a scatter plot of accuracy vs consistency.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and 'consistency' keys.
    """
    accuracies = [item['accuracy'] for item in data]
    consistencies = [item['consistency'] for item in data]
    labels = [item.get('label', '') for item in data]

    fig, ax = plt.subplots(figsize=(6, 5))
    xs = np.array(accuracies) * 100
    ys = np.array(consistencies) * 100
    scatter_icons(ax, xs, ys, labels, style, zoom=0.20)
    ax.set_xlabel('Accuracy (%)')
    ax.set_ylabel('Consistency (%)')

    ax.set_title('Scatter Plot of Accuracy vs Consistency')
    ax.set_xlim(30, 60)
    ax.set_ylim(80, 102)
    ax.grid(alpha=0.3)
    #ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    fig.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    fig.savefig(PLOTS_DIR_OUTPUT + 'accuracy_vs_consistency.png')

def create_scatter_accuracy_vs_token_cost(data):
    """
    Create a scatter plot of accuracy vs total token cost.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and 'total_token_cost' keys.
    """
    accuracies = [item['accuracy'] for item in data]
    token_costs = [item['total_token_cost'] for item in data]
    labels = [item.get('label', '') for item in data]

    fig, ax = plt.subplots(figsize=(6, 5))
    xs = np.array(accuracies) * 100
    ys = np.array(token_costs) / 1_000_000  # convert to millions
    scatter_icons(ax, xs, ys, labels, style, zoom=0.20)
    
    ax.set_xlabel('Accuracy')
    ax.set_ylabel('Total Token Cost of run (Millions)')
    ax.set_title('Scatter Plot of Accuracy vs Total Token Cost')
    ax.set_xlim(35, 60)
    ax.set_ylim(3, 7)
    ax.grid(alpha=0.3)
    #ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    fig.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    fig.savefig(PLOTS_DIR_OUTPUT + 'accuracy_vs_token_cost.png')

def create_scatter_accuracy_vs_latency(data):
    """
    Create a scatter plot of accuracy vs average latency.

    Args:
        data (list of dict): List containing dictionaries with 'accuracy' and 'average_tool_calls' keys.
    """
    accuracies = [item['accuracy'] for item in data]
    latencies = [item['average_tool_calls'] for item in data]
    labels = [item.get('label', '') for item in data]

    fig, ax = plt.subplots(figsize=(6, 5))
    xs = np.array(accuracies) * 100
    ys = np.array(latencies)
    scatter_icons(ax, xs, ys, labels, style, zoom=0.20)
    ax.set_xlabel('Accuracy')
    ax.set_ylabel('Average Tool Calls per run')
    ax.set_title('Scatter Plot of Accuracy vs Average Tool Calls')
    ax.set_xlim(20, 60)
    ax.set_ylim(6, 9)
    ax.grid(alpha=0.3)
    #ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.12), ncol=3)
    fig.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    fig.savefig(PLOTS_DIR_OUTPUT + 'accuracy_vs_latency.png')

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
    plt.ylim(5, 60)
    plt.grid(alpha=0.3)
    plt.tight_layout()
    if not os.path.exists(PLOTS_DIR_OUTPUT):
        os.makedirs(PLOTS_DIR_OUTPUT)
    plt.savefig(PLOTS_DIR_OUTPUT + f'{model_name}_config_accuracy_line_chart.png')

def print_table(data):
    """
    Print a formatted table of the data.

    Args:
        data (list of dict): List containing dictionaries with model metrics.
    """
    labels = [item.get('label', '') for item in data]
    accuracies = [item.get('accuracy', 0) for item in data]
    n_tokens = [item.get('total_token_cost', 0) for item in data]
    n_tool_calls = [item.get('average_tool_calls', 0) for item in data]
    print(f"{'Configuration':<30} {'Accuracy (%)':<15} {'Total Token Cost':<20} {'Avg Tool Calls':<15}")
    print("-" * 85)
    for label, acc, tokens, calls in zip(labels, accuracies, n_tokens, n_tool_calls):
        print(f"{label:<30} {acc*100:<15.2f} {tokens:<20.2f} {calls:<15.2f}")

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
    # Number of tool calls
    cleaned_data["average_tool_calls"] = data["tools"].apply(lambda l: len(l) if isinstance(l, list) else np.nan).mean()
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
    gpt4_1_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_FILENAME))
    gpt5_2_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT5_2_FILENAME))
    qwen30b_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_FILENAME))
    qwen480b_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN480B_FILENAME))

    qwen30b_bq_af_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FILENAME))
    qwen30b_bq_af_fbq_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_FILENAME))
    qwen30b_bq_af_fbq_acc_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_ACC_FILENAME))
    qwen30b_bq_af_fbq_acc_sav_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_ACC_SAV_FILENAME))
    qwen30b_bq_af_fbq_acc_sav_gh_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, QWEN30B_BQ_AF_FBQ_ACC_SAV_GH_FILENAME))
    qwen30b_full_data = qwen30b_data.copy()

    gpt4_1_wiki_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_WIKIDATA_FILENAME))
    gpt4_1_bq_af_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_BQ_AF_FILENAME))
    gpt4_1_bq_af_fbq_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_BQ_AF_FBQ_FILENAME))
    gpt4_1_bq_af_fbq_acc_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_BQ_AF_FBQ_ACC_FILENAME))
    gpt4_1_bq_af_fbq_acc_sav_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_BQ_AF_FBQ_ACC_SAV_FILENAME))
    gpt4_1_bq_af_fbq_acc_sav_gh_data = gpt4_1_data.copy(os.path.join(ORIGIN_DATA_DIR, GPT4_1_BQ_AF_FBQ_ACC_SAV_GH_FILENAME))
    gpt4_1_full_data = gpt4_1_data.copy()

    gpt4_1_sampling_off_dryrun_off_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_SAMPLINGOFF_DRYRUNOFF_FILENAME))
    gpt4_1_sampling_off_dryrun_on_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_SAMPLINGOFF_DRYRUNON_FILENAME))
    gpt4_1_sampling_on_dryrun_off_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_SAMPLINGON_DRYRUNOFF_FILENAME))
    gpt4_1_sampling_on_dryrun_on_data = pd.read_json(os.path.join(ORIGIN_DATA_DIR, GPT4_1_SAMPLINGON_DRYRUNON_FILENAME))

    # TEST ON DIFFERENT LLMS
    data_cleaned = []
    data_cleaned.append(clean_data_for_plotting(qwen30b_data, "QWEN-3 Coders 30B"))
    data_cleaned.append(clean_data_for_plotting(gpt4_1_data, "GPT-4.1"))
    data_cleaned.append(clean_data_for_plotting(gpt5_2_data, "GPT-5.2"))
    data_cleaned.append(clean_data_for_plotting(qwen480b_data, "QWEN-3 Coders 480B"))
    create_scatter_accuracy_consistency(data_cleaned)
    create_scatter_accuracy_vs_token_cost(data_cleaned)
    create_scatter_accuracy_vs_latency(data_cleaned)
    create_heatmap_by_complexity(data_cleaned, "Models Comparison by Question Complexity")
    create_stacked_bar_failure_types(data_cleaned)

    # TEST ON DIFFERENT CONFIGURATIONS OF THE SAME MODEL - QWEN-3 CODERS 30B
    qwen30b_data_cleaned_configs = []
    qwen30b_data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_data, "BQ + AF"))
    qwen30b_data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_data, "+ FBQ"))
    qwen30b_data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_acc_data, "+ ACC"))
    qwen30b_data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_acc_sav_data, "+ SAV"))
    qwen30b_data_cleaned_configs.append(clean_data_for_plotting(qwen30b_bq_af_fbq_acc_sav_gh_data, "+ GH"))
    qwen30b_data_cleaned_configs.append(clean_data_for_plotting(qwen30b_full_data, "+ AT"))
    line_chart_config_accuracy(qwen30b_data_cleaned_configs, "QWEN-3 Coders 30B")

    # TEST ON DIFFERENT CONFIGURATIONS OF THE SAME MODEL - GPT-4.1
    gpt4_1_data_cleaned_configs = []
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_wiki_data, "WIKIDATA"))
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_bq_af_data, "BQ + AF"))
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_bq_af_fbq_data, "+ FBQ"))
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_bq_af_fbq_acc_data, "+ ACC"))
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_bq_af_fbq_acc_sav_data, "+ SAV"))
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_bq_af_fbq_acc_sav_gh_data, "+ GH"))
    gpt4_1_data_cleaned_configs.append(clean_data_for_plotting(gpt4_1_full_data, "+ AT"))
    line_chart_config_accuracy(gpt4_1_data_cleaned_configs, "GPT-4.1")

    # ABLATION STUDY ON SAMPLING AND DRY RUN - GPT-4.1
    gpt4_1_ablation_data_cleaned = []
    gpt4_1_ablation_data_cleaned.append(clean_data_for_plotting(gpt4_1_sampling_off_dryrun_off_data, "Sampling OFF + Dry Run OFF"))
    gpt4_1_ablation_data_cleaned.append(clean_data_for_plotting(gpt4_1_sampling_off_dryrun_on_data, "Sampling OFF + Dry Run ON"))
    gpt4_1_ablation_data_cleaned.append(clean_data_for_plotting(gpt4_1_sampling_on_dryrun_off_data, "Sampling ON + Dry Run OFF"))
    gpt4_1_ablation_data_cleaned.append(clean_data_for_plotting(gpt4_1_sampling_on_dryrun_on_data, "Sampling ON + Dry Run ON"))
    print_table(gpt4_1_ablation_data_cleaned)