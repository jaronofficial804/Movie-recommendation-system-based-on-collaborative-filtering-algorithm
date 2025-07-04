document.addEventListener('DOMContentLoaded', function(){
    var btns = document.querySelectorAll('.del-comment');
    btns.forEach(function(btn){
        btn.onclick = function(){

            if(confirm('确定要删除这条评论吗？')){
                fetch('/delete_comment/'+this.dataset.cid, {method:'POST'})
                .then(r=>r.json()).then(data=>{
                    if(data.success) location.reload();
                    else alert(data.msg || '删除失败');
                });
            }
        }
    });
});
