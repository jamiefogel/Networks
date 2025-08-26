import numpy as np
from sklearn.neighbors import NearestNeighbors
import time

# ---------------------- PARAMETERS ------------------------
num_cores = 8       # Set the number of cores here (change as needed)
batch_size = 50000  # Number of queries processed at once (adjustable)

# ------------------------ DATA LOAD -----------------------
print("Loading data (float32)...")
P = np.load("prob_matrix_float32.npy", mmap_mode="r").astype(np.float32)

missing_idx = np.load("J_missing.npy")
pre_idx     = np.load("J_pre.npy")

P_pre  = P[pre_idx]
P_miss = P[missing_idx]

# ------------------ NEAREST NEIGHBORS SETUP -----------------
print("Setting up NearestNeighbors...")
nbrs = NearestNeighbors(
    metric="euclidean",
    algorithm="brute",
    n_jobs=num_cores
)
nbrs.fit(P_pre)

# ---------------------- MAIN LOOP -------------------------
hits = np.empty(len(missing_idx), dtype=np.int64)
num_batches = (len(missing_idx) + batch_size - 1) // batch_size

print(f"Starting computation: {num_batches} batches of size {batch_size}")

start_time = time.time()
for i, start in enumerate(range(0, len(missing_idx), batch_size), 1):
    end = min(start + batch_size, len(missing_idx))
    block = P_miss[start:end]

    t0 = time.time()
    _, ix = nbrs.kneighbors(block, n_neighbors=1, return_distance=True)
    hits[start:end] = pre_idx[ix.ravel()]
    t1 = time.time()

    elapsed = t1 - start_time
    est_total = elapsed / i * num_batches
    print(f"[Batch {i}/{num_batches}] "
          f"Jobs {start:,}-{end-1:,} done. "
          f"Batch time: {t1 - t0:.2f}s | "
          f"Elapsed: {elapsed/60:.2f} min | "
          f"ETA: {(est_total - elapsed)/60:.2f} min")

# -------------------- SAVE RESULTS ------------------------
np.save("nearest_pre_indices.npy", hits)
print("Finished! Results saved as 'nearest_pre_indices.npy'.")
