---
title: ForeseeBench Automation
emoji: 📊
colorFrom: blue
colorTo: gray
sdk: gradio
sdk_version: 6.31.0
python_version: 3.12.12
app_file: deploy/hf_space/app.py
pinned: false
short_description: Resumable forecast-data collection and runtime status
---

# ForeseeBench automation

This Space exposes runtime status. Data and checkpoints remain in private ForeseeBench HF resources.

Deployment starts in isolated qualification mode. Production requires verified recovery and a transfer of exclusive write ownership.
UptimeRobot polls `/health`: verified healthy operation returns 200; a missing valid receipt or runtime failure returns 503.

Source, protocol, and current acceptance status: [ForeseeBench-Automation](https://github.com/JiaWANG-TJ/ForeseeBench-Automation).
