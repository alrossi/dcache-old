function checkAll(bx, clzz) {
	var cbs = document.getElementsByClassName(clzz);
	for (var i = 0; i < cbs.length; i++) {
		if (cbs[i].type == 'checkbox') {
			cbs[i].checked = bx.checked;
		}
	}
}