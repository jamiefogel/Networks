import sys

# Access the parameter value passed as a command-line argument
parameter_value = sys.argv[1]

parameter_value = int(parameter_value) + 1

# Print or use the result as needed
print("Result: " + str(parameter_value) +" out of " + str(sys.argv[2]))
