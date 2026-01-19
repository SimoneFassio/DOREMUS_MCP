
import ast
import sys

def count_tool_docstrings(filename):
    with open(filename, 'r') as f:
        # Read file but be careful with async keywords if python version < 3.8
        tree = ast.parse(f.read())

    total_chars = 0
    tools_found = []

    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            is_tool = False
            for decorator in node.decorator_list:
                # We need to handle @mcp.tool() (Call) and @mcp.tool (Attribute)
                
                # Case 1: @mcp.tool() - It's a Call
                if isinstance(decorator, ast.Call):
                    func = decorator.func
                    # func should be an Attribute (mcp.tool)
                    if isinstance(func, ast.Attribute) and func.attr == 'tool':
                        # Optionally check if value is 'mcp'
                        if isinstance(func.value, ast.Name) and func.value.id == 'mcp':
                            is_tool = True
                
                # Case 2: @mcp.tool - It's an Attribute directly (if no parens)
                # (Though source code shows parens, checking this doesn't hurt)
                elif isinstance(decorator, ast.Attribute):
                    if decorator.attr == 'tool':
                         if isinstance(decorator.value, ast.Name) and decorator.value.id == 'mcp':
                            is_tool = True

            if is_tool:
                docstring = ast.get_docstring(node)
                if docstring:
                    # ast.get_docstring cleans it.
                    length = len(docstring)
                    total_chars += length
                    tools_found.append((node.name, length))
                else:
                    tools_found.append((node.name, 0))

    print(f"{'Tool Name':<30} | {'Length':<10}")
    print("-" * 45)
    for name, length in tools_found:
        print(f"{name:<30} | {length:<10}")
    print("-" * 45)
    print(f"{'Total':<30} | {total_chars:<10}")

if __name__ == "__main__":
    count_tool_docstrings("src/server/main.py")
