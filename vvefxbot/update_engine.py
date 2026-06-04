import re

with open('backtest/engine.py', 'r') as f:
    content = f.read()

# Read the new function from scratch
with open('scratch_engine.py', 'r') as f:
    new_func = f.read()

# Add indentation
new_func = '\n'.join(['    ' + line if line else line for line in new_func.split('\n')])

# Pattern to find the existing _check_exits function
# It starts at "    def _check_exits(" and ends before "    # ------------------------------------------------------------------" 
# which is followed by "# HELPERS"
pattern = r"    def _check_exits\(.*?(?=    # ------------------------------------------------------------------\n    # HELPERS)"

# Check if pattern matches
if re.search(pattern, content, re.DOTALL):
    content = re.sub(pattern, new_func + "\n", content, flags=re.DOTALL)
    with open('backtest/engine.py', 'w') as f:
        f.write(content)
    print("Successfully replaced _check_exits")
else:
    print("Could not find the function block to replace")
