export HF_TOKEN=${HF_TOKEN}

sudo sync
echo 3 | sudo tee /proc/sys/vm/drop_caches

docker run --rm -it \
          --gpus all \
         --network host \
          --ipc=host \
              --ulimit memlock=-1 \
        --ulimit stack=67108864 \
            -v ~/.cache/huggingface:/root/.cache/huggingface \
            -e HF_HOME=/root/.cache/huggingface \
             -e HF_TOKEN=$HF_TOKEN \
         nvcr.io/nvidia/vllm:26.05-py3 \
           vllm serve Qwen/Qwen3-Embedding-8B \
            --served-model-name qwen3-embedding-8b \
           --runner pooling \
            --dtype auto \
            --quantization fp8 \
            --max-model-len 8192 \
            --gpu-memory-utilization 0.9 \
           --max-num-seqs 256 \
            --max-num-batched-tokens 32768 \
           --enforce-eager \
            --host 0.0.0.0 \
            --port 8000
root@ubuntu:~#
