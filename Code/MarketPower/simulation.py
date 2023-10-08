import torch
import random

# Define the dimensions of the matrix
J = 3  # Number of rows
I = 4  # Number of columns

# Generate a random matrix with values between 0 and 1
random_matrix = torch.rand(J, I)

# Scale the random matrix to generate positive values
min_value = 0.1  # Minimum positive value
max_value = 1.0  # Maximum positive value

# Scale and shift the random matrix to the desired range
positive_matrix = random_matrix * (max_value - min_value) + min_value

print(positive_matrix)
