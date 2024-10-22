import concurrent.futures

# def process_posts_in_parallel(posts, max_workers=10,model=None, task= None, client=None):
#     results = []
#     openai_client = client

#     with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
#         future_to_index = {executor.submit(task, post, model = model, openai_client=openai_client): idx for idx, post in enumerate(posts)}
#         for future in concurrent.futures.as_completed(future_to_index):
#             idx = future_to_index[future]
#             try:
#                 result = future.result()
#                 results.append((idx, result))
#             except Exception as exc:
#                 print(f'Post at index {idx} generated an exception: {exc}')
#                 results.append((idx, "skipped"))
    
#     # Sort results by the original index
#     results.sort(key=lambda x: x[0])
#     return [result for _, result in results]
def process_posts_in_parallel(posts, max_workers=10, model=None, task=None, client=None):
    results = []

    def wrapped_task(post):
        try:
            return task(post, model=model, client=client)
        except Exception as exc:
            print(f'Post generated an exception: {exc}')
            return "skipped"

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {executor.submit(wrapped_task, post): idx for idx, post in enumerate(posts)}
        for future in concurrent.futures.as_completed(future_to_index):
            idx = future_to_index[future]
            try:
                result = future.result()
                results.append((idx, result))
            except Exception as exc:
                print(f'Future at index {idx} generated an exception: {exc}')
                results.append((idx, "skipped"))
    
    # Sort results by the original index
    results.sort(key=lambda x: x[0])
    return [result for _, result in results]