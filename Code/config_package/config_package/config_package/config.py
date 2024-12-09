import os
import sys
import platform
import getpass

homedir = os.path.expanduser('~')
os_name = platform.system()
user = getpass.getuser()

if user == 'p13861161':
    if os_name == 'Windows':
        print("Running on Windows") 
        root = "//storage6/usuarios/labormkt_rafaelpereira/NetworksGit/"
        rais = "//storage6/bases/DADOS/RESTRITO/RAIS/"
        sys.path.append(root + 'Code/Modules')
        sys.path.append(r'C:\ProgramData\anaconda3\Lib\site-packages\src')
    elif os_name == 'Linux':
        print("Running on Linux") 
        root = "/home/DLIPEA/p13861161/labormkt/labormkt_rafaelpereira/NetworksGit/"
        rais = "/home/DLIPEA/p13861161/rais/RAIS/"
        sys.path.append(root + 'Code/Modules')

elif user == 'jfogel':
    print("Running on Jamie's home laptop")
    root = homedir + '/NetworksGit/'

# Append the Modules directory to sys.path
sys.path.append(root + 'Code/Modules')

# Define the directory for figures
figuredir = root + 'Results/'