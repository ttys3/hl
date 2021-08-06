# Sorting workflows

## Regular file

### Current implementation

1. Main thread
    1. Filter blocks (by timestamp and level filters)
    2. Sort blocks in chronological order by first record timestamp
2. Pusher thread
    1. Read and push blocks one by one
3. Worker threads
    1. Take next incoming block and split it into lines
    2. Parse lines as records
    3. Filter records
    4. If there are no records, go to (1)
    5. Format records into a preallocated buffer
    6. Collect formatted records ranges
    7. Push the formatted block
4. Merger thread
    1. Pull next formatted block
    2. Put the block into workspace
    3. Sort all blocks in a workspace by next record timestamp
    4. Copy the first record of the first block
    5. If its timestamp is >= than timestamp of recenly pulled formatted block, go to (1), else go to (3)

## Named pipe or standard input

### Current implementation

1. Reader thread
    1. Read input splitting into blocks
    2. Archive each block and store into memory
2. Indexer threads
    1. Index each block
3. Pusher thread
    1. Filter blocks (by timestamp and level filters)
    2. Sort blocks in chronological order by first record timestamp
    3. Read and push blocks one by one
4. Worker threads
    1. Take next incoming block and split it into lines
    2. Parse lines as records
    3. Filter records
    4. If there are no records, go to (1)
    5. Format records into a preallocated buffer
    6. Collect formatted records ranges
    7. Push the formatted block
5. Merger thread
    1. Pull next formatted block
    2. Put the block into workspace
    3. Sort all blocks in a workspace by next record timestamp
    4. Copy the first record of the first block
    5. If its timestamp is >= than timestamp of recenly pulled formatted block, go to (1), else go to (3)

### Desired implementation

1. Reader thread
    1. Read input splitting into blocks
2. Parser threads
    1. Take next incoming block and split it into lines
    2. Parse lines as records
    3. Filter records (if no filter, (3)..(6) may be skipped)
    4. If there are no records, go to (1)
    5. Copy records to a new block
    6. Collect ranges of the records
    7. Archive block and store it into memory
    8. Push the metadata of the block down to the pipeline
    9. Go to (1)
3. Pusher thread
    1. Collect all incoming blocks
    2. Filter blocks (by timestamp and level filters) (? - duplicates 2.3)
    3. Sort blocks in chronological order by first message timestamp
    4. Read and push blocks one by one
4. Worker threads
    1. Take next incoming block and split it into lines
    2. Parse lines as records
    3. Filter records (? - duplicates 2.3)
    4. If there are no records, go to (1)
    5. Format records into a preallocated buffer
    6. Collect formatted records ranges
    7. Push the formatted block
5. Merger thread
    1. Pull next formatted block
    2. Put the block into workspace
    3. Sort all blocks in a workspace by next record timestamp
    4. Copy the first record of the first block
    5. If its timestamp is >= than timestamp of recenly pulled formatted block, go to (1), else go to (3)

## Compressed file

### Desired implementation

1. Main thread
    1. Filter blocks (by timestamp and level filters)
    2. Evaluate chronological order by first record timestamp
    3. Evaluate block lifetimes along the chronological stream
2. Reader thread
    1. Read next input block
    2. Drop if it is filtered in (1.1)
    3. Push it down to the pipeline
3. Parser threads
    1. Take next incoming block and split it into lines
    2. Parse lines as records
    3. Filter records (if no filter, (3)..(6) may be skipped)
    4. If there are no records, go to (1)
    5. If it goes in chronological order, push it down the pipeline and go to (1)
    6. Copy records to a new block
    7. Collect ranges of the records
    8. Archive block and store it into memory
    9. Push the metadata of the block down to the pipeline
    10. Go to (1)
4. Pusher thread
    1. Collect all incoming blocks
    2. Filter blocks (by timestamp and level filters) (? - duplicates 3.3)
    3. Sort blocks in chronological order by first message timestamp
    4. Read and push blocks one by one
5. Worker threads
    1. Take next incoming block and split it into lines
    2. Parse lines as records
    3. Filter records (? - duplicates 3.3)
    4. If there are no records, go to (1)
    5. Format records into a preallocated buffer
    6. Collect formatted records ranges
    7. Push the formatted block
6. Merger thread
    1. Pull next formatted block
    2. Put the block into workspace
    3. Sort all blocks in a workspace by next record timestamp
    4. Copy the first record of the first block
    5. If its timestamp is >= than timestamp of recenly pulled formatted block, go to (1), else go to (3)


