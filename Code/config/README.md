# Config Package

This document explains how to set up and install the `config` package for managing platform-specific configurations on both Windows and Linux. This package allows seamless handling of file paths and system-dependent settings.

---

## Package Contents

The package includes:

- `config.py`: Contains platform-specific configurations.
- `__init__.py`: Allows importing key variables directly from the package.
- `setup.py`: Used to install the package.
- Additional files: `README.md` and `LICENSE`.

---

## Installation Instructions

### Prerequisites
1. **Python** (version 3.6 or higher) must be installed on your system.
2. A virtual environment (e.g., `conda` or `venv`) must be active for the installation.

---

### Steps to Install on **Windows**

1. **Activate the Environment:**
   Open the **Anaconda PowerShell Prompt** and activate the `labor_new` environment:
   ```powershell
   conda activate labor_new
   ```

2. **Navigate to the Package Directory:**
   Change to the directory where `setup.py` is located. For example:
   ```powershell
   cd \storage6\usuarios\labormkt_rafaelpereira\NetworksGit\Code\config
   ```

3. **Install the Package:**
   Run the following command to install the package in the active environment:
   ```powershell
   pip install .
   ```

4. **Verify the Installation:**
   Open Python and test the package:
   ```python
   from config import homedir, root, rais, figuredir
   print(homedir, root, rais, figuredir)
   ```

---

### Steps to Install on **Linux**

1. **Activate the Environment:**
   Open a terminal and activate the `labor_gt` Conda environment:
   ```bash
   conda activate labor_gt
   ```

2. **Navigate to the Package Directory:**
   Change to the directory where `setup.py` is located. For example:
   ```bash
   cd /home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/Code/config
   ```

3. **Install the Package:**
   Run the following command to install the package in the active environment:
   ```bash
   pip install .
   ```

4. **Verify the Installation:**
   Open Python and test the package:
   ```python
   from config import homedir, root, rais, figuredir
   print(homedir, root, rais, figuredir)
   ```

---

## Key Features of the `config` Package

The `config.py` file dynamically configures file paths and other settings based on:
- The operating system (`Windows` or `Linux`).
- The username (`p13861161`, `jfogel`, etc.).

It includes:
- `homedir`: The home directory of the current user.
- `os_name`: The name of the operating system.
- `root`: The base directory for your project.
- `rais`: The path to the RAIS data.
- `figuredir`: The path for saving figures.

---

## Uninstalling the Package

To uninstall the package from your environment, run:
```bash
pip uninstall config
```

---

## Troubleshooting

### **UNC Path Issues on Windows**
If the target path uses a UNC network path (e.g., `\\storage6\...`) and causes errors like "UNC paths are not supported," you can map the path to a drive letter:
1. Open a terminal and map the drive:
   ```powershell
   net use Z: \\storage6\usuarios\labormkt_rafaelpereira
   ```
2. Use the mapped drive (`Z:\Code`) instead of the UNC path.

### **Reinstalling the Package**
If you need to reinstall the package, use:
```bash
pip install . --force-reinstall
```

---

## Credits

This package was created to handle platform-specific configurations for projects that span multiple operating systems. Feel free to adapt or extend it to suit your needs.

