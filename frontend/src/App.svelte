<script>
  import { roundFlex } from "./utils/utils";
  import svelteLogo from "./assets/svelte.svg";
  import viteLogo from "/vite.svg";
  import { onDestroy, onMount } from "svelte";
  import Router from "svelte-spa-router";
  import Navigation from "./components/Navigation.svelte";
  import Home from "./routes/Home.svelte";
  import ChartView from "./components/ChartView.svelte";
  // 초기 데이터 설정
  let initialData = [{ BLDG_NM: "Loading...", prediction: 0, THING_AMT: 0 }];
  let data = [];

  let searchQuery = "";
  let socket;
  let loading = true;
  let filterData = [];
  const defaultYear = new Date().getFullYear().toString();
  // let loading = true;
  const routes = {
    "/": Home,
  };
  onMount(() => {
    socket = new WebSocket("ws://127.0.0.1:8000/ws");
    // websocket 연결
    socket.addEventListener("open", () => {
      console.log("연결 성공");
      loading = false;
      socket.send(JSON.stringify({ query: null, year: defaultYear }));
    });
    socket.onmessage = (evt) => {
      const receivedData = JSON.parse(evt.data);
      data = receivedData;
      filterData = data.slice(0, 10);
      loading = false;
    };

    window.onbeforeunload = () => {
      try {
        if (socket.readyState === WebSocket.OPEN) {
          console.log("close web");
          socket.close();
        }
      } catch (error) {
        console.log("ws error", error);
      }
    };
  });
  async function fetchData(year) {
    try {
      const rep = await fetchDataz();
    } catch (error) {
      console.log(error);
    }
  }
  // 검색 버튼 클릭
  function handleSearch() {
    if (!searchQuery.trim()) {
      alert("검색어 입력");
      return;
    }
    loading = false;
    data = [];
    socket.send(JSON.stringify({ query: searchQuery, year: defaultYear }));
    filterData = [];
  }
</script>

<Navigation />

<main>
  <div class="container">
    <h1 class="my-4">부동산 실거래</h1>
    <ChartView {data} {initialData} />
    <!-- 검색창 추가 -->
    <div class="row mb-3">
      <div class="col">
        <label for="search_input" class="form-label">날짜 조회</label>
        <div class="input-group">
          <input
            type="text"
            id="search_input"
            class="form-control"
            placeholder="날짜 조회"
            bind:value={searchQuery}
          />
          <button class="btn btn-outline-secondary" on:click={handleSearch}>
            검색
          </button>
        </div>
      </div>
    </div>

    <div class="table-responsive mt-4">
      <table class="table">
        <thead>
          <tr>
            <th>접수연도</th>
            <th>건물 이름</th>
            <th>계약일</th>
            <th>실거래가(만원)</th>
            <th>예측 가격</th>
            <th>건물 용도</th>
          </tr>
        </thead>
        <tbody>
          {#if !loading && filterData.length > 0}
            {#each filterData as row}
              <tr>
                <td>{row.RCPT_YR}</td>
                <td>{row.BLDG_NM}</td>
                <td>{row.CTRT_DAY}</td>
                <td>{row.THING_AMT}</td>
                <td>{roundFlex(row.prediction)}</td>
                <td>{row.BLDG_USG}</td>
              </tr>
            {/each}
          {/if}
        </tbody>
      </table>
    </div>
  </div>
</main>

<style>
  main {
    padding-top: 60px;
  }
</style>
