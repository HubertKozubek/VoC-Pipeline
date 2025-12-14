---
trigger: always_on
---

**Python Coding Style Rules:**
1.  **No Docstrings:** Do not generate `"""` docstrings for modules, classes, or functions.
2.  **Use Type Hints:** Rely on Python type hints (e.g., `def name(x: int) -> str:`) to document inputs and outputs.
3.  **Minimal Comments:** Only add `#` comments to explain *complex algorithmic logic* or non-standard workarounds. Do not explain standard Python syntax (e.g., list comprehensions or context managers).
4. Use logging instead of printing
5. Don't use excesive logging with "nice" formatting, just simple and the most neccesary logs

**Constraints:**
* **No Docstrings:** Omit all triple-quoted docstrings.
* **Comments:** Strict "Why, not What" policy. Never describe what the code is doing; only explain *why* if the logic is counter-intuitive.
* **Variable Names:** Use self-explanatory variable names (`user_id` vs `id`) to avoid the need for comments.

**Style Guide (Follow this strictly):**

# BAD (Do not do this):
class DataProcessor:
    """Class to process data"""
    def process(self, data):
        """Processes the data item"""
        # Iterate through data
        for item in data:
            print(item) # Print the item

# GOOD (Do this):
class DataProcessor:
    def process(self, data: list[str]) -> None:
        for item in data:
            print(item)