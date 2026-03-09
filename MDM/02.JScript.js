/**
 * HR Master Code Management System
 * Purpose: Manages hierarchical HR standard codes and their detailed attributes.
 * Framework: iStudio Low-code Platform (Matrix)
 */

// Object Definitions
var mainGrid = Matrix.getObject("DataGrid");        // Main category information grid
var detailGrid = Matrix.getObject("DataGrid1");     // Sub-detail information grid
var subCodeState = Matrix.getObject("VS_SUBCODE");  // State variable to pass DetailCode to the sub-grid
var lblUserCode = Matrix.getObject("LB_사번");      // User identification label

/*****************************************************************
 * Event: OnLoadComplete
 * Description: Initializes data when the document is fully loaded.
 *****************************************************************/
var OnLoadComplete = function(sender, args) {
    // Synchronize initial data for the main category grid
    Matrix.doRefresh("DataGrid"); 
    
    // Set current user identity for audit trail or session tracking
    lblUserCode.Text = Matrix.GetUserInfo().UserCode;
};

/*****************************************************************
 * Event: OnButtonClick
 * Description: Routing logic for UI actions (Search, Add, Save).
 *****************************************************************/
var OnButtonClick = function(sender, args) {
    
    // --- Main Category Grid Actions ---
    if (args.Id == "Search") {
        Matrix.doRefresh("DataGrid");
    } 
    else if (args.Id == "Save") {
        if (mainGrid.IsModified()) {
            // Execute Database Transaction for Main Category (Plan_1)
            Matrix.ExecutePlan("PLAN_1", "", function(p) {
                if (!p.Success) {
                    Matrix.Alert(p.Message);
                    return;
                }
                mainGrid.ClearRowState(); // Reset modification flags
                Matrix.Information("Changes saved successfully.", "Success");
                Matrix.doRefresh("DataGrid");
            });
        } else {
            alert("No changes detected in the Main Category.");
        }
    } 
    else if (args.Id == "Add") {
        mainGrid.AppendRow();
    }

    // --- Detail Information Grid Actions ---
    else if (args.Id == "Search_Detail") {
        Matrix.doRefresh("DataGrid1");
    }
    else if (args.Id == "Save_Detail") {
        if (detailGrid.IsModified()) {
            // Execute Database Transaction for Detail Data (Plan_2)
            Matrix.ExecutePlan("PLAN_2", "", function(p) {
                if (!p.Success) {
                    Matrix.Alert(p.Message);
                    return;
                }
                detailGrid.ClearRowState();
                Matrix.Information("Detail changes saved successfully.", "Success");
                Matrix.doRefresh("DataGrid1");
            });
        } else {
            alert("No changes detected in the Detail Information.");
        }
    }
    else if (args.Id == "Add_Detail") {
        detailGrid.AppendRow();
    }
    else if (args.Id == "Delete_Detail") {
        Matrix.Confirm("Are you sure you want to delete the selected records?", "Confirm Delete", function(isOk) {
            if (isOk) {
                detailGrid.RemoveRow();
            }
        });
    }
};

/*****************************************************************
 * Event: OnCellClick
 * Description: Handles Master-Detail synchronization. 
 * Updates the sub-grid headers and filters based on the selected row.
 *****************************************************************/
var OnCellClick = function(sender, args) {
    // Trigger synchronization when the "DetailCode" column is clicked
    if (args.Field.Caption == "상세코드" || args.Field.Name == "DetailCode") {
        if (args.Id == "DataGrid") {
            
            // 1. Update the filter key for the Detail Grid
            var selectedCode = args.Row.GetCell("상세코드").Value;
            subCodeState.Text = selectedCode;
            Matrix.doRefresh("DataGrid1");

            // 2. Dynamic Header Mapping
            // Sync column captions of the detail grid with the metadata defined in the main grid
            for (var i = 1; i <= 10; i++) {
                var fieldKey = "VALUE" + i;
                var metaValue = args.Row.GetValue(fieldKey);
                
                // If metadata exists, set it as the header; otherwise, use the default caption
                if (metaValue && metaValue !== "") {
                    detailGrid.GetField(fieldKey).Caption = metaValue;
                } else {
                    detailGrid.GetField(fieldKey).Caption = mainGrid.GetField(fieldKey).Caption;
                }
            }
            
            // Refresh UI components to apply dynamic header changes
            detailGrid.Update();
        }
    }
};
