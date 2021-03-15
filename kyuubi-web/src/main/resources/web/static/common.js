function logout() {
    $.ajax({
        url: ctxPath + "/rest/logout",
        dataType:'json',
        success: function () {
            window.location.href = ctxPath + "/rest/login"
        }
    })
}