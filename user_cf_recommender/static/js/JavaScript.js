window.onload = function () {
    // 检查localStorage中是否有登录状态
    const isLoggedIn = localStorage.getItem("loggedIn") === "true";

    // 根据登录状态显示相应内容
    document.getElementById('login_box').style.display = isLoggedIn ? 'none' : 'block';
    document.getElementById('app_content').style.display = isLoggedIn ? 'block' : 'none';
};