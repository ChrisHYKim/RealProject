<script>
    export let currentPage = 1;
    export let itemsPerPage = 10;
    export let data = [];
    // 페이지 번호 변경
    function handlePageChange(page) {
        if (page < 1 || page > totalPages) return;
        currentPage = page;
        // 부모 컴포넌트로 페이지 변경 이벤트를 전달
        dispatch("pageChange", currentPage);
    }

    const totalPages = Math.ceil(data.length / itemsPerPage);
</script>

<div class="pagination">
    <button
        on:click={() => handlePageChange(currentPage - 1)}
        disabled={currentPage === 1}
    >
        이전
    </button>

    {#each Array(totalPages) as _, i}
        <button
            on:click={() => handlePageChange(i + 1)}
            class:selected={i + 1 === currentPage}
        >
            {i + 1}
        </button>
    {/each}

    <button
        on:click={() => handlePageChange(currentPage + 1)}
        disabled={currentPage === totalPages}
    >
        다음
    </button>
</div>

<style>
    .pagination {
        display: flex;
        justify-content: center;
        align-items: center;
        margin-top: 20px;
    }

    .pagination button {
        margin: 0 5px;
        padding: 5px 10px;
    }

    .pagination button.selected {
        font-weight: bold;
        background-color: #007bff;
        color: white;
    }

    .pagination button:disabled {
        background-color: #ddd;
    }
</style>
