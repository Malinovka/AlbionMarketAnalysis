function loadTable() {
$.getJSON('PROFIT.json', function(data){
    $.each(data, function(key, value){
    marketorder = '<tr>' + '<td>' + value.item + '</td>' + '<td>' + value.from + '</td>' + '<td>' + value.to + '</td>' + '<td>' + value.quantity + '</td>' + '<td>' + value.profit + '</td>'  + '</tr>';
    $('#marketorders').append(marketorder);
    });
    $('#marketorders').DataTable({        
        "paging": false,
        "order": [[ 4, "desc" ]],        
    })
    $('.dataTables_length').addClass('bs-select');
});
};
