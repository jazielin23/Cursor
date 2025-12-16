# install_tensorflow.R
# Run this in a FRESH R session (close Shiny first), on Windows.
#
# Why: keras/tensorflow installation can fail once Python DLLs are loaded.
#
# IMPORTANT: Your current py_config shows Python 3.13; TensorFlow often does NOT
# ship wheels for 3.13 yet. Prefer Python 3.11 or 3.12.

# Ensure packages
if (!requireNamespace("reticulate", quietly = TRUE)) install.packages("reticulate")
if (!requireNamespace("keras", quietly = TRUE)) install.packages("keras")

# Optional: point reticulate at a specific Python (edit this if you installed Python 3.12/3.11)
# Sys.setenv(RETICULATE_PYTHON = "C:/Path/To/Python312/python.exe")

cat("\n=== reticulate::py_config() BEFORE ===\n")
print(reticulate::py_config())

cat("\n=== Installing keras + tensorflow (keras::install_keras) ===\n")
keras::install_keras()

cat("\n=== reticulate::py_config() AFTER ===\n")
print(reticulate::py_config())

cat("\n=== TensorFlow module available? ===\n")
print(reticulate::py_module_available("tensorflow"))

cat("\nDone. Restart the Shiny app after this completes.\n")
