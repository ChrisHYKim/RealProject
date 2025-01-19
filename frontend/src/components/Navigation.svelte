<script>
    import { link } from "svelte-spa-router";
    let windowsWidth = window.innerWidth;

    let clicked = false;

    function handlResize() {
        windowsWidth = window.innerWidth;
        if (windowsWidth >= 1024) clicked = false;
    }
</script>

<svelte:window on:resize={handlResize} />
<!-- navbar 생성 -->
<nav class="navbar navbar-expand-lg navbar-light bg-light border-bottom">
    <div class="container-fluid">
        <a use:link class="navbar-brand" href="/main">RealEate</a>
        <button
            class="navbar-toggler"
            type="button"
            data-bs-toggle="collapse"
            data-bs-target="#navbarSupportedContent"
            aria-controls="navbarSupportedContent"
            aria-expanded="false"
            aria-label="Toggle navigation"
        >
            <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarSupportedContent">
            <ul class="navbar-nav me-auto mb-2 mb-lg-0">
                <li class="nav-item">
                    <a use:link class="nav-link" href="/main">실거래가</a>
                </li>
                <li class="nav-item">
                    <a use:link class="nav-link" href="/">매매가</a>
                </li>
            </ul>
        </div>
    </div>
</nav>

<style>
    nav {
        position: fixed;
        top: 0;
        left: 0;
        width: 100%;
        z-index: 1000;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
</style>
