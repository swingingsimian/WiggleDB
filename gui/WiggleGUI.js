//////////////////////////////////////////
// Global configuration
//////////////////////////////////////////

var CGI_URL = "http://" + location.hostname + "/cgi-bin/wiggleCGI.py?";
var attribute_values_file = "datasets.attribs.json";

//////////////////////////////////////////
// Main function 
//////////////////////////////////////////

$(document).ready(main)

function main() {
  $.getJSON(CGI_URL + "annotations=1").done(get_annotations).fail(catch_JSON_error);
}

function get_annotations(data) {
  annotations = data['annotations'];
  create_all_selectors();
  add_annotations();
  define_buttons();
}

//////////////////////////////////////////
// Global variables
//////////////////////////////////////////

var selection_panels = [
  "choose",
  "chooseB",
  "chooseA2",
  "chooseA"
];

var panel_letters = {
  "choose":"A",
  "chooseB":"B",
  "chooseA2":"A",
  "chooseA":"A"
};

var attribute_values = null;

var annotations = null;

var reduction_opts = {"Intersection":"unit mult", "Union":"unit sum"};

var comparison_opts = {"Intersection":"unit mult", "Union":"unit sum", "Difference": "unit diff"};

var annotation_opts = {"Intersection":"unit mult", "Union":"unit sum", "Difference": "unit diff", "Overlap frequency": "overlaps"};

var selection_opts = {"That overlap":"overlaps", "That don't overlap":"noverlaps", "That are within": "overlaps extend", "That a farther than": "noverlaps extend"};

//////////////////////////////////////////
// Creating multiselects 
//////////////////////////////////////////

function add_value_to_multiselect(value, div) {
  $("<option>").attr("value",value).text(value).appendTo(div);
}

function create_multiselect(container, attribute, panel) {
  var multiselect2 = $("<select>")
    .addClass("multiselect")
    .attr("multiple","multiple")
    .appendTo(container)
    .attr("attribute",panel_letters[panel.attr("id")]+ "_" + attribute);

  if (attribute in attribute_values) {
    attribute_values[attribute].map(function(value) {add_value_to_multiselect(value, multiselect2);});
  }
  multiselect2.multiselect({onChange: function(element, checked) {update_panel_count(panel);}, maxHeight: 400, buttonWidth:'100%'});
  multiselect2.parent().find('.btn').css("white-space","normal");
}

function all_selects_are_used(panel) {
  var selects = panel.find(".form-control");
  var res = true; 
  selects.each(function(rank, select) {if ($(select).val() == "None") {res = false;}});
  return res;
}

function change_multiselect() {
  var select = $(this);
  var panel = select.parents("[id*='choose']");
  var col = select.parent().parent().find(".multiselect").parent(".form-group");
  if (select.val() in attribute_values) {
    col.children().remove();
    create_multiselect(col, select.val(), panel);
  } else {
    col.parent().remove();
  }
  
  if (all_selects_are_used(panel)) {
    create_selection_div(panel);
  }
}

function create_attribute_select(container) {
  var select = $("<select>").addClass("form-control").appendTo(container);
  Object.keys(attribute_values).map(function(attribute) {add_attribute_to_select(attribute, select);});
  $("<option>").attr("value","None").text("None").attr("selected","selected").appendTo(select);
  select.change(change_multiselect);
}

function add_attribute_to_select(attribute, select) {
  $("<option>").attr("value",attribute).text(attribute).appendTo(select);
}

function update_panel(panel) {
  // Compute initial count:
  update_panel_count(panel);
  // Set reduction select options
  update_panel_reduction(panel);
  // Comparison select:
  update_tab_comparison(panel.parents('.tab-pane'));
}

function create_selection_div(panel) {
  // Top most container
  var row = $("<div>").addClass("row").appendTo(panel.find("#selection"));

  // Division into fixed width columns
  var col1 = $("<div>").addClass("form-group").addClass("col-md-5").appendTo(row);
  $("<div>").addClass("form-group").addClass("col-md-1").text(" is: ").appendTo(row);
  var col2 = $("<div>").addClass("form-group").addClass("col-md-5").appendTo(row);

  // Create attribute selector in column 1:
  create_attribute_select(col1);
  // Create empty value selector in column 2:
  create_multiselect(col2, "None", panel);

  update_panel(panel);
}

function all_filters_used(panel) {
  var res = true;
  panel.find(".filter").each(
    function (index, obj) {
      if ($(obj).find("#constraint").val() == "(No filter)") {
        res = false;
      }
    }
  );
  return res;
}

function reset_filter() {
  var panel = $(this).parents(".tab-pane");
  if ($(this).val() == "(No filter)" && panel.find(".filter").length > 1) {
    $(this).parents(".filter").remove();
  }
  if (all_filters_used(panel)) {
    create_filter_div(panel);
  }
}

function create_filter_div(panel) {
  var row = $('<div>').attr('class','filter row').appendTo(panel.find(".filters"));

  // Division into fixed width columns
  var col1 = $("<div>").addClass("form-group").addClass("col-md-4").appendTo(row);
  var col2 = $("<div>").addClass("form-group").text(" bp from ").addClass("col-md-4").appendTo(row);
  var col3 = $("<div>").addClass("form-group").addClass("col-md-4").appendTo(row);

  var verb = $("<select>").addClass("form-control").attr('id','constraint').appendTo(col1);
  $("<option>").attr("value","overlaps").text("are within").attr("selected","selected").appendTo(verb);
  $("<option>").attr("value","noverlaps").text("are farther than").attr("selected","selected").appendTo(verb);
  $("<option>").attr("value",null).text("(No filter)").attr("selected","selected").appendTo(verb);

  $("<input>").attr('type','text').attr('id','distance').attr('style','width: 50%;').change(update_my_tab).prependTo(col2);

  var object = $("<select>").addClass("form-control").attr('id','reference').appendTo(col3);
  if (annotations != null) {
    annotations.map(function(attribute) {add_attribute_to_select(attribute, object);});
  }

  verb.change(reset_filter);
}

function create_panel(panel) {
  create_selection_div(panel);
  create_filter_div(panel);
}

function create_all_selectors() {
  jQuery.getJSON(attribute_values_file).done(function(values) {
    attribute_values = values;
    selection_panels.map(function (id) {create_panel($("#"+id));});
  }).fail(catch_JSON_error);
}

//////////////////////////////////////////
// Creating the reference selectors 
//////////////////////////////////////////

function display_provenance(data) {
  var modal = $('#Provenance_modal').clone();
  modal.find('#myModalLabel').text(data["name"]);
  modal.find('#myModalBody').text(data["description"]);
  modal.modal();
}

function get_provenance() {
  var dataset = $(this).attr('value');
  jQuery.getJSON(CGI_URL + "provenance=" + dataset).done(display_provenance).fail(catch_JSON_error);
}

function add_annotation(annotation) {
  var row = $("<tr>").appendTo($('#refs'));
  $("<td>").appendTo(row).append($("<input>").attr("class", "select_annot").attr("type","checkbox").attr("value", annotation));
  $("<td>").appendTo(row).text("\t" + annotation);
  $("<td>").appendTo(row).attr("align","right").append($("<input>").attr("id","source").attr("type","image").attr("src","images/info.png").attr("value",annotation));
}

function add_annotations() {
  $("#refs").on("click","#source", get_provenance);
  fill_select($('#reference_reduction'), reduction_opts);
  annotations.map(add_annotation);
}

//////////////////////////////////////////
// Panel Query
//////////////////////////////////////////

function panel_query(panel) {
  var string = [];
  panel.find(".multiselect option:selected").each(
    function (index, element) {
      var variable = $(element).parent().attr("attribute");
      var value = $(element).val();  
      if (value != null && value != "") {
        string.push(variable+'='+value);
      } 
    }
  );
  return string.join("&");
}

///////////////////////////////////////////
// Panel reduction
///////////////////////////////////////////

function panel_reduction(panel) {
  var commands = [];
  panel.find('.filter').each(
    function (index, div) {
      var constraint = $(div).find("#constraint").val();
      var distance = $(div).find("#distance").val();
      var reference = $(div).find("#reference").val();
      if (constraint == "(No filter)") {
	return;
      } else if (distance == null || distance == "") {
	commands.push([constraint, reference].join(" ")); 
      } else {
	commands.push([constraint, "extend", distance, reference].join(" ")); 
      } 
    }
  );
  commands.push(panel.find('#reduction').val());
  return commands.join(" ");
}

//////////////////////////////////////////
// Computing panel selection count
//////////////////////////////////////////

function update_panel_count(panel) {
  $.getJSON(CGI_URL + "count=true&" + panel_query(panel)).done(
    function(data, textStatus, jqXHR) {
      panel.find("#count").text("(" + data["count"] + " elements selected)");
    }
  ).fail(catch_JSON_error);
}

//////////////////////////////////////////
// Updating panel reduction select 
//////////////////////////////////////////

function fill_select(select, options) {
  select.children().remove();
  Object.keys(options).map(function(opt) {$("<option>").attr("value",options[opt]).text(opt).appendTo(select);});
}

function update_my_tab() {
  update_tab_comparison($(this).parents('.tab-pane'));
}

function update_panel_reduction(panel) {
  fill_select(panel.find('#reduction'), reduction_opts)
}

//////////////////////////////////////////
// Updating tab comparison select 
//////////////////////////////////////////

function update_tab_comparison(tab) {
  if (tab.attr('id') == 'annotate') {
    fill_select(tab.find("#annotation"), annotation_opts);
  } else {
    fill_select(tab.find("#comparison"), comparison_opts);
  }
}

//////////////////////////////////////////
// Button up!
//////////////////////////////////////////

function define_buttons() {
  $('#summary_button').click(summary);
  $('#comparison_button').click(comparison);
  $('#annotation_button').click(annotation);
  $('#upload_button').click(upload_dataset);
}

function update_my_panel() {
  $(this).addClass('active');
  var panel = $(this).parents('[id*="choose"]');
  if (panel.length == 0) {
    return;
  }
  var X = 1;
  update_panel(panel);
  $(this).removeClass('active');
}

function report_result(data) {
  if (data["status"] == "DONE") {
    if (data['url'].substr(-4,4) == ".txt") {
      var modal = $('#Image_modal').clone();
      modal.find('#url').attr('href',data['url']);
      modal.find('img').attr('src',data['view']);
      modal.find('#photo_url').attr('href',data['view']);
      modal.modal();
    } else if (data['url'].substr(-3,3) == '.bw' || data['url'].substr(-4,4) == '.bb') {
      var modal = $('#Success_modal').clone();
      modal.find('#url').attr('href',data['url']);
      modal.find('#view').attr('href',data['view']);
      modal.modal();
    }
  } else if (data["status"] == "EMPTY") {
    $('#Empty_modal').modal();	
  } else if (data["status"] == "INVALID") {
    $('#Invalid_modal').modal();	
  } else if (data["status"] == "ERROR") {
    $('#Failure_modal').modal();	
  }
}

// Get result
function get_result() {
  $.getJSON(CGI_URL + "result=" + $('#result_box').val()).done(report_result).fail(catch_JSON_error);
}

// Upload user dataset
function upload_dataset() {
  $.getJSON(CGI_URL + "uploadUrl=" + $('#uploadURL').val() + "&description=" + $("#uploadDescription").val()).done(report_upload).fail(catch_JSON_error);
}

function report_upload(data) {
  if (data["status"] == "UPLOADED") {
      var modal = $('#Success_upload_modal').clone();
      modal.modal();
  } else if ("format" in data) {
      var modal = $('#Malformed_upload_modal').clone();
      modal.find('#url').text(data['url']);
      modal.find('#format').text(data['format']);
      modal.modal();
  } else {
      var modal = $('#Failed_upload_modal').clone();
      modal.find('#url').text(data['url']);
      modal.find('#format').text(data['format']);
      modal.modal();
  }
}

// Send job to server 
function submit_query(query) {
  $.getJSON(CGI_URL + query).done(report_result).fail(catch_JSON_error);
}

// Request summary
function summary() {
  var panel = $('#choose');
  submit_query(panel_query(panel) + '&wa=' + panel_reduction(panel)); 
}

// Request comparison
function comparison() {
  var comparison = $('#comparison').val();
  var panelA = $('#chooseA');
  var panelB = $('#chooseB');
  submit_query(
    [ 
      panel_query(panelA),
      panel_query(panelB),
      'wa='+panel_reduction(panelA),
      'wb='+panel_reduction(panelB),
      'w='+comparison
    ].join("&")
  ); 
}

// Request annotation
function annotation() {
  var comparison = $('#annotation').val();
  var panelA = $('#chooseA2');
  var annots = [];
  $('#refs').find(".select_annot").each(function (rank, obj) {if (obj.checked) annots.push($(obj).attr("value"));});
  submit_query(
    [ 
      panel_query(panelA),
      annots.map(function (str) {return "B_annot_name="+str}).join("&"),
      'wa='+panel_reduction(panelA),
      'w='+comparison
    ].join("&")
  ); 
}

// JSON error handler
function catch_JSON_error(jqXHR, textStatus, errorThrown) {
  console.log('JSON failed: ' + textStatus + ":" + errorThrown + ":" + jqXHR.responseText + ":" + jqXHR.getAllResponseHeaders());
}
