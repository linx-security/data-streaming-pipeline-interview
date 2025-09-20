# Senior Team Lead Coding Test

**Duration: 60 minutes**
**Focus: Real-time Stream Processing, Async Programming, Error Handling**

## 🎯 Your Challenge

You need to implement a **real-time data processing pipeline** that can handle streaming data from multiple sources
concurrently while dealing with rate limits and failures gracefully.

## 📋 What You Need to Do

### 1. Implement the `StreamProcessor` class

In the file `data_streaming_pipeline_test.py`, you'll find a `StreamProcessor` class with empty methods. Your job is to
implement all the methods to make the pipeline work.

### 2. Key Requirements

Your implementation must:

- ✅ **Process multiple data streams concurrently** (not one at a time)
- ✅ **Handle rate limiting** - respect the downstream API's rate limits without crashing
- ✅ **Recover from failures** - API calls will fail ~15% of the time, handle this gracefully
- ✅ **Provide real-time metrics** - track processing statistics as data flows through
- ✅ **Process efficiently** - achieve minimum 5 requests/sec throughput
- ✅ **Manage backpressure** - handle when data comes in faster than you can process

### 3. What Success Looks Like

When you run `python data_streaming_pipeline_test.py`, you should see:

```
🚀 Testing Stream Processing Pipeline...
📊 Starting stream processing for 10 seconds...
✅ Processing completed!
📈 Total processed: 45
⚡ Processing rate: 6.2/sec
🕐 Avg processing time: 150.3ms
❌ Error rate: 12.5%
📦 Final backlog: 3
🚦 Rate limit hits: 2
🎉 All performance requirements met!
```

**Performance Targets:**

- **Processing rate**: ≥ 5/sec
- **Error rate**: ≤ 30%
- **Backlog size**: ≤ 100 items

## 🏗️ Architecture Overview

```
Data Streams → [Your StreamProcessor] → Downstream API
     ↓                    ↓                    ↓
 Multiple sources    Process & validate    Rate limited
 Real-time data      Handle failures       Random failures
```

**Pipeline Flow:**
1. Consume data from the provided `data_stream`
2. Transform each `StreamData` using `transform_data()`
3. Send `ProcessedRecord` to `downstream_api.send_data()`
4. Track metrics throughout the process
5. Handle rate limits and failures with retries

## 🔧 Files You'll Work With

### `data_streaming_pipeline_test.py` - **YOUR MAIN FILE**

- Contains the `StreamProcessor` class you need to implement
- Has a built-in test runner at the bottom
- **This is the only file you should modify**

### `streaming_test_framework.py` - **DO NOT MODIFY**

- Contains all the supporting infrastructure (models, simulators, etc.)
- Provides the data types you'll work with:
    - `StreamData` - incoming data from streams
    - `ProcessedRecord` - your processed output
    - `PipelineMetrics` - statistics you need to track
    - `DownstreamAPI` - the API you send data to (with rate limits and failures)

## 💡 Key Concepts

Your solution will need to handle:

- **Streaming data processing** - continuous data flow from multiple sources
- **Error resilience** - APIs fail, your system shouldn't
- **Performance constraints** - meet throughput requirements under realistic conditions
- **Resource management** - handle concurrent processing efficiently

## 🚀 Getting Started

1. **Understand the interface**: Look at the `StreamProcessor` class methods
2. **Run the test first**: `python data_streaming_pipeline_test.py` (it will fail initially)
3. **Implement step by step**: Start with basic functionality, then add concurrency and error handling
4. **Test frequently**: Run the test after each major change to see progress
5. **Focus on the core challenge**: Async stream processing with concurrency and resilience

## ⚠️ Important Notes

- **Only modify** `data_streaming_pipeline_test.py`
- **Don't modify** `streaming_test_framework.py`
- **You can use** standard Python libraries (asyncio, collections, etc.)
- **Ask questions** if anything is unclear about the requirements
- **Focus on working code first**, then optimize for performance

---

**Good luck! Focus on building a robust, concurrent stream processor. Remember: working code first, then optimization.**
🚀
