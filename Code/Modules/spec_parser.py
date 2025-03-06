# spec_parser.py

import sys
import os

def parse_spec(root):
    # Validate command line arguments
    if len(sys.argv) != 2:
        print("Usage: python <script.py> <spec>")
        sys.exit(1)

    chosen_spec = sys.argv[1]

    # Append the directory containing your metafile module.
    # Adjust the path as necessary.
    sys.path.append(os.path.join(root, 'Code/replicate_mayara/Felix_JMP/'))
    
    # Import the specs from metafile after updating sys.path
    from metafile import specs
    
    # Build a dictionary for faster lookup
    spec_dict = {spec["name"]: spec for spec in specs}

    # Check if chosen_spec is valid
    if chosen_spec not in spec_dict:
        print(f"Spec '{chosen_spec}' not recognized. Options are: {', '.join(spec_dict.keys())}")
        sys.exit(1)

    # Extract variables from the selected spec
    market_vars = spec_dict[chosen_spec]["market_vars"]
    file_suffix = spec_dict[chosen_spec]["file_suffix"]
    _3states    = spec_dict[chosen_spec]["_3states"]

    print(f"Running spec: {chosen_spec}")
    print(f"Market variables: {market_vars}")
    print(f"File suffix: {file_suffix}")
    print(f"_3states: {_3states}")

    return chosen_spec, market_vars, file_suffix, _3states
