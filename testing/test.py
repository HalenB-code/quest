import subprocess
import pathlib
import messages as rbdc_test

exe = pathlib.Path(__file__).cwd() / r'target\debug\rust-bdc.exe'
print(f"Target executable: {exe}")

# Start the executable process
args = [r"C:\rust\projects\rust-bdc\config.toml"]
process = subprocess.Popen(
    [exe, args[0]],  # Replace with your actual executable path
    stdin=subprocess.PIPE, 
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,  # Enables text mode (handles input/output as strings)
    bufsize=1,  # Line buffering for stdout
)

# Define the requests to send
nodes = ["n1", "n2", "n3", "n4", "n5"]
testing = rbdc_test.Message(nodes)
requests = testing.build_messages_ordered(iterations=1)

# Send requests and read responses
for req in requests:
    print(f"Request: {req}")

    process.stdin.write(req)
    process.stdin.flush()  # Ensure the request is sent immediately

    # Read response (assuming the executable responds line-by-line)
    response = process.stdout.readline().strip()
    print(f"Response: {response}")

# Close the process when done
process.stdin.close()
process.wait()
