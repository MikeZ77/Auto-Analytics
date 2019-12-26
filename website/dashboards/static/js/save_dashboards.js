function getCookies(){
  var cookie_list = decodeURIComponent(document.cookie).split(';');
  // If there are cookies:
  if (cookie_list[0]!==""){
    for (var i=0; i<cookie_list.length; i+=2){
      var dropdown_item = cookie_list[i].split('=')
      var link = dropdown_item[1].slice(1,-1)
      var dropdown_item = cookie_list[i+1].split('=')
      var title = dropdown_item[1].slice(1,-1)

      $('.dropdown-menu').prepend('<a class="dropdown-item"></a>');
      $('.dropdown-item').first().attr('href',link);
      $('.dropdown-item').first().text(title);
    }
  }
}

function clearCookies(){
  var cookies = document.cookie.split(";");

  // for (var i = 0; i < cookies.length; i++) {
  //   var cookie = cookies[i];
  //   var eqPos = cookie.indexOf("=");
  //   var name = eqPos > -1 ? cookie.substr(0, eqPos) : cookie;
  //   document.cookie = name + "=;expires=Thu, 01 Jan 1970 00:00:00 GMT";
  // }
}
