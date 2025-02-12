# -*- coding: utf-8 -*-
"""
Created on Thu Jan 30 18:28:33 2025

@author: p13861161
"""

import pandas as pd
import numpy as np
from config import root, rais
import time
import psutil
import graph_tool.all as gt
import gc
import matplotlib.pyplot as plt
import pickle
import os
import multiprocessing
import threading


# ------------------------------------------------------------------------------
# 1) LOADING DATA (original function, unchanged)
# ------------------------------------------------------------------------------
def load_data():
    dfs = []
    # Should this start with 86?
    for year in range(1987, 1991):
        df = pd.read_parquet(f'/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/replicate_mayara/monopsonies/sas/rais{year}.parquet')
        
        df = df[['fakeid_worker', 'fakeid_firm', 'fakeid_estab', 'agegroup', 'cbo', 'municipality']]
        df['state'] =  pd.to_numeric(df['municipality'].astype(str).str[:2], errors='coerce').astype('Int64')
    
        # Create jid only for rows where both fakeid_estab and occ4 are not missing
        df['fakeid_estab'] = df['fakeid_estab'].astype('Int64')
        df.loc[df['fakeid_estab'].notna() & df['cbo'].notna(), 'jid'] = df['fakeid_estab'].astype(str) + '_' + df['cbo'].astype(str).str[0:4]
        df.loc[df['fakeid_estab'].isna() | df['cbo'].isna(), 'jid'] = pd.NA
            
        dfs.append(df)
    
    stacked = pd.concat(dfs, ignore_index=True)
    edgelist = stacked.loc[stacked.jid.notna(),['fakeid_worker','jid']]
    edgelist.to_pickle(root + '/Data/derived/mayara_edgelist_1987_1990.p')


# ------------------------------------------------------------------------------
# 2) BUILD BIPARTITE GRAPH (original function, unchanged)
# ------------------------------------------------------------------------------
def build_bipartite_graph(df, 
                          worker_col="fakeid_worker", 
                          job_col="jid",
                          drop_multi_edges=True,
                          min_job_degree=1):
    """
    Build a bipartite graph-tool.Graph from a pandas DataFrame containing
    [worker_col, job_col] edges.
    
    Parameters
    ----------
    df : pd.DataFrame
        Must contain columns [worker_col, job_col].
    worker_col : str
        Name of the column containing worker IDs.
    job_col : str
        Name of the column containing job IDs.
    drop_multi_edges : bool
        If True, drop duplicate (worker, job) pairs.
    min_job_degree : int
        If > 1, filter out job nodes with fewer than this number of distinct workers.
    
    Returns
    -------
    g : graph_tool.Graph
        Bipartite graph-tool graph.
    worker_map : dict
        Maps worker ID -> graph-tool vertex index.
    job_map : dict
        Maps job ID -> graph-tool vertex index.
    """
    # 1) Clean the DataFrame
    df = df[[worker_col, job_col]].dropna()
    
    # 2) Optionally remove job nodes with fewer than min_job_degree edges
    if min_job_degree > 1:
        job_counts = df.groupby(job_col)[worker_col].nunique()
        valid_jobs = job_counts[job_counts >= min_job_degree].index
        df = df[df[job_col].isin(valid_jobs)]
    
    # 3) Optionally drop multi-edges
    if drop_multi_edges:
        df = df.drop_duplicates([worker_col, job_col])
    
    # 4) Create the bipartite graph
    g = gt.Graph(directed=False)
    worker_map = {}
    job_map = {}
    
    v_type = g.new_vertex_property("int")
    g.vertex_properties["bipartite_type"] = v_type
    
    # Helpers to add worker/job vertices
    def get_worker_vertex(wid):
        if wid in worker_map:
            return worker_map[wid]
        else:
            v = g.add_vertex()
            worker_map[wid] = v
            v_type[v] = 0   # worker type
            return v

    def get_job_vertex(jid):
        if jid in job_map:
            return job_map[jid]
        else:
            v = g.add_vertex()
            job_map[jid] = v
            v_type[v] = 1   # job type
            return v
    
    # 5) Add edges
    for w, j in zip(df[worker_col], df[job_col]):
        v_w = get_worker_vertex(w)
        v_j = get_job_vertex(j)
        g.add_edge(v_w, v_j)
    
    return g, worker_map, job_map


# ------------------------------------------------------------------------------
# 3) TEST SBM INFERENCE (original function, unchanged)
# ------------------------------------------------------------------------------
def test_sbm_inference(g, sample_fraction=0.1):
    # Create a subgraph if needed
    if sample_fraction < 1.0:
        e_list = list(g.edges())
        np.random.shuffle(e_list)
        num_sample = int(len(e_list) * sample_fraction)
        e_sub = e_list[:num_sample]

        g_sub = gt.Graph(directed=False)
        old2new = {}
        v_type_sub = g_sub.new_vertex_property("int")
        g_sub.vertex_properties["bipartite_type"] = v_type_sub

        def get_new_vertex(old_v):
            if old_v in old2new:
                return old2new[old_v]
            else:
                v = g_sub.add_vertex()
                old2new[old_v] = v
                v_type_sub[v] = g.vp.bipartite_type[old_v]
                return v

        for e in e_sub:
            src = get_new_vertex(e.source())
            tgt = get_new_vertex(e.target())
            g_sub.add_edge(src, tgt)
        g_test = g_sub
    else:
        g_test = g

    num_edges = g_test.num_edges()
    num_nodes = g_test.num_vertices()
    print(f"Sample fraction: {sample_fraction}")
    print(f"Graph has {num_nodes} nodes and {num_edges} edges.")

    # -------------------------------
    # (A) Identify the Giant Component
    # -------------------------------
    comp, hist = gt.label_components(g_test, directed=False)
    # comp[v] = ID of the component that vertex v belongs to
    # hist[c] = number of vertices in component c
    largest_comp_id = np.argmax(hist)
    # Build a GraphView that keeps only vertices in the largest component
    vfilt_gc = (comp.a == largest_comp_id)  # Boolean mask
    g_gc = gt.GraphView(g_test, vfilt=vfilt_gc)

    gc_num_nodes = g_gc.num_vertices()
    gc_num_edges = g_gc.num_edges()

    gc_node_fraction = gc_num_nodes / num_nodes if num_nodes > 0 else 0
    gc_edge_fraction = gc_num_edges / num_edges if num_edges > 0 else 0



    # Measure memory usage before
    process = psutil.Process()
    mem_before = process.memory_info().rss / (1024**3)  # in GB
    
    start_time = time.time()
    state = gt.minimize_nested_blockmodel_dl(g_test)
    elapsed_time = time.time() - start_time
    
    # Measure memory usage after
    mem_after = process.memory_info().rss / (1024**3)
    mem_inference = mem_after - mem_before

    print(f"SBM inference took {elapsed_time:.2f} seconds.")
    print(f"Memory before: {mem_before:.2f} GB, after: {mem_after:.2f} GB, increase: {mem_inference:.2f} GB")
    return {
        "sample_fraction": sample_fraction,
        "num_nodes": num_nodes,
        "num_edges": num_edges,
        "gc_node_fraction": gc_node_fraction,  # fraction of nodes in giant component
        "gc_edge_fraction": gc_edge_fraction,  # fraction of edges in giant component
        "elapsed_time": elapsed_time,
        "mem_before": mem_before,
        "mem_after": mem_after,
        "mem_inference": mem_inference
    }


# ------------------------------------------------------------------------------
# 4) MONITOR RAM IN A SEPARATE THREAD
# ------------------------------------------------------------------------------
def monitor_ram(process, fraction, ram_file, ram_list, interval=60):
    """
    Continuously monitors RAM usage while 'process' is running.
    """
    with open(ram_file, "a") as f:
        while process.is_alive():
            ram_usage = round(psutil.virtual_memory().used / (1024 ** 3), 1)
            timestamp = time.time()
            ram_list.append(ram_usage)
            f.write(f"{fraction},{timestamp},{ram_usage}\n")
            f.flush()
            time.sleep(interval)


# ------------------------------------------------------------------------------
# 5) WRAPPER TO RUN SBM INFERENCE IN A CHILD PROCESS
# ------------------------------------------------------------------------------
def run_sbm(fraction, return_dict):
    """
    Runs the test_sbm_inference function and stores the result or any error.
    """
    try:
        result = test_sbm_inference(g, sample_fraction=fraction)
        return_dict['result'] = result
    except Exception as e:
        return_dict['error'] = str(e)


# ------------------------------------------------------------------------------
# 6) COMBINED TEST WITH TIMEOUT
#    - Smaller samples likely finish before timeout
#    - Larger samples might get truncated
# ------------------------------------------------------------------------------
def run_combined_test(g, output_file, ram_usage_file, sample_sizes, monitor_interval=60, timeout=2*3600):
    """
    For each fraction in sample_sizes:
      - Runs SBM in a separate process (with 'timeout').
      - Monitors RAM usage in a separate thread.
      - If a process times out, kills it and marks that run as incomplete.
    """
    results = []
    ram_usage_results = []

    for fraction in sample_sizes:
        print(f"\n[Combined Test] Running SBM on sample fraction: {fraction}")
        
        # Shared dictionary to capture results from the child process
        manager = multiprocessing.Manager()
        return_dict = manager.dict()
        
        # Launch child process
        process = multiprocessing.Process(target=run_sbm, args=(fraction, return_dict))
        process.start()

        # Launch RAM monitor (thread)
        ram_list = []
        ram_monitor_thread = threading.Thread(
            target=monitor_ram,
            args=(process, fraction, ram_usage_file, ram_list, monitor_interval)
        )
        ram_monitor_thread.start()

        # Record start time so we know how long it ran (only if it completes)
        start_time = time.time()

        # Wait for the process (up to 'timeout' seconds)
        process.join(timeout=timeout)

        # Compute elapsed time (only valid if not terminated)
        elapsed_time = time.time() - start_time

        if process.is_alive():
            print(f"Sample fraction {fraction} exceeded timeout. Terminating process.")
            process.terminate()
            process.join()
            return_dict['result'] = None  # Mark no result
            return_dict['error'] = "Timeout reached"
            # For clarity, set the runtime to None or NaN
            elapsed_time = np.nan

        # Stop RAM monitor thread
        ram_monitor_thread.join()

        # Prepare record for this fraction
        # The 'result' from 'test_sbm_inference()' is in return_dict['result']
        # If it was None, then the job timed out or had an error.
        run_outcome = return_dict.get('result', None)
        
        if run_outcome is not None:
            # We have a successful result
            run_error = None
            # Overwrite the original 'elapsed_time' from inside test_sbm_inference
            # only if you prefer the "wall time" (here 'elapsed_time' might differ slightly
            # from what test_sbm_inference itself measured, but typically they match).
            run_outcome["elapsed_time"] = run_outcome["elapsed_time"]
        else:
            # Timed out or error
            run_error = return_dict.get('error', None)

        results.append({
            'fraction': fraction,
            'result': run_outcome,
            'error': run_error,
            'elapsed_time': run_outcome["elapsed_time"] if run_outcome else np.nan,
            'ram_usage': ram_list
        })

        ram_usage_results.append({'fraction': fraction, 'ram_used': ram_list})

        # Save partial results to disk
        with open(output_file, "wb") as f:
            pickle.dump(results, f)
        pd.DataFrame(ram_usage_results).to_csv(ram_usage_file, index=False)
    
    return results, ram_usage_results


# ------------------------------------------------------------------------------
# 7) MAIN EXECUTION
# ------------------------------------------------------------------------------
if __name__ == "__main__":
    # (Optional) If you need to create the edgelist first:
    # load_data()

    # Load the pre-made edgelist
    edgelist = pd.read_pickle(root + '/Data/derived/mayara_edgelist_1987_1990.p')
    
    # Build the bipartite graph
    g, worker_map, job_map = build_bipartite_graph(
        edgelist,
        worker_col="fakeid_worker",
        job_col="jid",
        drop_multi_edges=True,
        min_job_degree=1
    )
    print(f"Created graph with {g.num_vertices()} vertices and {g.num_edges()} edges.")
    
    # Clean up memory
    del edgelist
    gc.collect()
    
    current_ram = psutil.virtual_memory().used / (1024 ** 3)
    print(f"Current system RAM usage: {current_ram:.1f} GB")
    
    # Define sample fractions (small to large)
    sample_sizes = np.concatenate([
        np.linspace(0.0001, 0.01, num=13), 
        np.array([0.03, 0.06, 0.09])
    ])
    
    
    # Paths for saving results
    output_file = root + '/Data/derived/combined_sbm_results_v4.pkl'
    ram_usage_file = root + "/Data/derived/combined_sbm_ram_usage_v4.csv"
    
    # Make sure RAM usage file has a header if it doesn't exist
    if not os.path.exists(ram_usage_file):
        with open(ram_usage_file, "w") as f:
            f.write("fraction,timestamp,ram_used\n")
    
    print("\n=== Starting Combined Test (with timeouts) ===")
    combined_results, combined_ram_usage = run_combined_test(
        g,
        output_file=output_file,
        ram_usage_file=ram_usage_file,
        sample_sizes=sample_sizes,
        monitor_interval=60,   # record RAM usage every 60s
        timeout=12*3600         # 2-hour timeout
    )
    print("\nCombined test completed.")


    # ------------------------------------------------------------------------------
    # 8) PLOT RUNTIME vs. SAMPLE FRACTION FOR COMPLETED RUNS
    # ------------------------------------------------------------------------------
    # Build a DataFrame from the combined_results
    # 'combined_results' is a list of dicts with keys: ['fraction','result','error','elapsed_time','ram_usage']
    # 'result' is either None or a dict from test_sbm_inference()
    # If 'result' is not None, it has: [sample_fraction, num_nodes, num_edges, elapsed_time, mem_before, mem_after, mem_inference]
    
    # Convert to a DataFrame with one row per fraction
    df_combined = []
    for entry in combined_results:
        frac = entry["fraction"]
        err = entry["error"]
        res = entry["result"]
        ram = max(entry["ram_usage"])
        if res is not None:
            df_combined.append({
                "fraction": frac,
                "elapsed_time": res["elapsed_time"],
                "num_nodes": res["num_nodes"],
                "num_edges": res["num_edges"],
                "gc_node_fraction": res["gc_node_fraction"],
                "gc_edge_fraction": res["gc_edge_fraction"],
                "ram": ram,
                "error": err
            })
        else:
            # If timed out or error, store NaNs for numeric fields
            df_combined.append({
                "fraction": frac,
                "elapsed_time": np.nan,
                "num_nodes": np.nan,
                "num_edges": np.nan,
                "mem_inference": np.nan,
                "error": err
            })
    
    df_combined = pd.DataFrame(df_combined)
    # Sort by fraction for nicer plotting
    df_combined = df_combined.sort_values(by="fraction", ascending=True)

    # Filter out rows that have no valid runtime
    df_completed = df_combined.dropna(subset=["elapsed_time"])

    # Now we can plot "elapsed_time" vs. "fraction" for the completed runs
    if not df_completed.empty:
        plt.figure(figsize=(8,6))
        plt.plot(
            df_completed["fraction"], 
            df_completed["elapsed_time"], 
            marker='o', 
            linestyle='-', 
            label="Completed Runs"
        )
        plt.xlabel("Sample Fraction")
        plt.ylabel("Runtime (seconds)")
        plt.title("SBM Inference Runtime vs. Sample Fraction (Completed Only)")
        plt.grid(True)
        plt.legend()
        plt.show()
    else:
        print("No runs completed before timeout to plot runtime.")

    print("Done plotting runtime for completed runs.")
