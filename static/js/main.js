$(function() {
    $('a#live').click(function(f) {
        f.preventDefault();
        var page = $(this).attr('href');
        $('#mainId').load(page);
    })
});

