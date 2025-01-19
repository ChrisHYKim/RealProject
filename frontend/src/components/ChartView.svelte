<script>
    import { roundFlex } from "../utils/utils";
    import { afterUpdate, onMount } from "svelte";
    import { Chart } from "chart.js/auto";
    export let data = [];
    export let initialData = [];
    let filterData = [];

    // 데이터 슬라이스 진행
    let pagination;
    let chart;
    let isLoading = true;
    // 날짜 중복 제거
    function removeDupDate(date) {
        const uniqueData = [];
        const seenDates = new Set();

        filterData.forEach((row) => {
            if (!seenDates.has(row.CTRT_DAY)) {
                seenDates.add(row.CTRT_DAY);
                uniqueData.push(row);
            }
        });

        return uniqueData;
    }
    // chart 구성
    function updateChart(chartData) {
        if (!chartData || chartData.length === 0) return;
        isLoading = false;
        // 날짜 중복 검도
        const uniqueData = removeDupDate(chartData);

        const labels = uniqueData.map((row) => row.BLDG_NM);
        const pre_data = uniqueData.map((row) => roundFlex(row.prediction));
        const cityPrice = uniqueData.map((row) => row.THING_AMT);
        const minPrices = uniqueData.map((row) =>
            Math.min(roundFlex(row.prediction), row.THING_AMT),
        );
        const maxPrices = uniqueData.map((row) =>
            Math.max(roundFlex(row.prediction), row.THING_AMT),
        );
        const allPrices = [...minPrices, ...maxPrices];
        const minRange = Math.min(...allPrices);
        const maxRange = Math.max(...allPrices);
        const ctx = document.getElementById("priceChart").getContext("2d");

        if (chart) {
            chart.destroy();
        }
        chart = new Chart(ctx, {
            type: "bar",
            data: {
                labels: labels,
                datasets: [
                    {
                        label: "예측 가격",
                        data: pre_data,
                        backgroundColor: "rgba(75, 192, 192, 0.2)",
                        borderColor: "rgba(75, 192, 192, 1)",
                        borderWidth: 1,
                    },
                    {
                        label: "실제 가격",
                        data: cityPrice,
                        backgroundColor: "rgba(255, 99, 132, 0.2)",
                        borderColor: "rgba(255, 99, 132, 1)",
                        borderWidth: 1,
                    },
                    {
                        label: "최저 가격",
                        data: minPrices,
                        backgroundColor: "rgba(255, 159, 64, 0.2)",
                        borderColor: "rgba(255, 159, 64, 1)",
                        borderWidth: 1,
                        type: "line",
                        fill: false,
                    },
                    {
                        label: "최대 가격",
                        data: maxPrices,
                        backgroundColor: "rgba(153, 102, 255, 0.2)",
                        borderColor: "rgba(153, 102, 255, 1)",
                        borderWidth: 1,
                        type: "line", // 최대 가격은 선 차트로 표시
                        fill: false,
                    },
                ],
            },
            options: {
                responsive: true,
                scales: {
                    y: {
                        beginAtZero: true,
                        suggestedMax: minRange - 1000,
                        suggestedMin: maxRange + 1000,
                        stacked: false,
                    },
                },
            },
        });
    }
    onMount(() => {
        updateChart(initialData);
    });
    afterUpdate(() => {
        if (data.length > 0) {
            filterData = data.slice(0, 10);
            updateChart(filterData);
            filterData = [];
        }
    });
</script>

<div class="row">
    <div class="col-md-12">
        {#if isLoading}
            <div class="loading-circle">Chart..</div>
        {/if}
        <canvas
            id="priceChart"
            style={isLoading ? "visibility: hidden;" : "visibility: visible;"}
        ></canvas>
    </div>
</div>

<style>
    canvas {
        max-width: 100%;
        height: auto;
    }
    /* 로딩 원 스타일 */
    .loading-circle {
        position: absolute;
        left: 50%;
        top: 50%;
        transform: translate(-50%, -50%);
        border: 4px solid #f3f3f3;
        border-top: 4px solid #3498db;
        border-radius: 50%;
        width: 40px;
        height: 40px;
        animation: spin 2s linear infinite;
    }
    @keyframes spin {
        0% {
            transform: rotate(0deg);
        }
        100% {
            transform: rotate(360deg);
        }
    }
</style>
