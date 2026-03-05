/**
 * Project: HR Master Data Management (MDM) System
 * Description: Client-side logic for Synchronizing Master/Detail HR Codes.
 * Framework: BI-Matrix Enterprise UI Engine
 */

// --- UI Component References ---
var masterGrid     = Matrix.getObject("MasterGrid");     // Primary Dataset Grid
var detailGrid     = Matrix.getObject("DetailGrid");     // Dependent Dataset Grid
var selectionState = Matrix.getObject("SelectionState"); // State Variable for Filtering
var lblUserSession = Matrix.getObject("lblUserSession"); // UI Component for Session Info

/**
 * Event: System Initialization
 * Purpose: Prepares the UI state upon document load.
 */
var OnLoadComplete = function(sender, args)
{
    // Display current user identity from session metadata
    lblUserSession.Text = Matrix.GetUserInfo().UserCode;
    
    // Initial data synchronization for the Master Grid
    Matrix.doRefresh("MasterGrid");
};

/**
 * Event: Centralized Action Handler
 * Purpose: Handles CRUD operations and data persistence triggers.
 */
var OnButtonClick = function(sender, args)
{
    switch(args.Id) 
    {
        // --- Master Grid Actions ---
        case "btnSearch": 
            Matrix.doRefresh("MasterGrid");
            break;

        case "btnSaveMaster":
            if (masterGrid.IsModified()) {
                Matrix.ExecutePlan("PLAN_SAVE_MASTER", "", function(p) {
                    if (p.Success === false) {
                        Matrix.Alert(p.Message);
                        return;
                    }
                    masterGrid.ClearRowState();
                    Matrix.Information("Master record saved successfully.", "Success");
                    Matrix.doRefresh("MasterGrid");
                });
            } else {
                alert("No changes detected in the Master Grid.");
            }
            break;

        case "btnAddMaster": // Replaced 'Append' with 'Add' for clarity
            masterGrid.AppendRow();
            break;

        // --- Detail Grid Actions ---
        case "btnSaveDetail":
            if (detailGrid.IsModified()) {
                Matrix.ExecutePlan("PLAN_SAVE_DETAIL", "", function(p) {
                    if (p.Success === false) {
                        Matrix.Alert(p.Message);
                        return;
                    }
                    detailGrid.ClearRowState();
                    Matrix.Information("Detail records saved successfully.", "Success");
                    Matrix.doRefresh("DetailGrid");
                });
            } else {
                alert("No changes detected in the Detail Grid.");
            }
            break;

        case "btnAddDetail": // Replaced 'Append' with 'Add'
            detailGrid.AppendRow();
            break;

        case "btnDeleteDetail":
            Matrix.Confirm("Are you sure you want to delete the selected detail record(s)?", "Confirm Deletion", function(isOk) {
                if (isOk) {
                    detailGrid.RemoveRow();
                }
            });
            break;
    }
};

/**
 * Event: Master-Detail Synchronization Logic
 * Purpose: Implements dynamic metadata mapping for the Detail Grid based on Master selection.
 */
var OnCellClick = function(sender, args)
{
    // Condition: Interaction on the 'DetailCode' column within the Master Grid
    if (args.Field.Caption === "DetailCode" && args.Id === "MasterGrid") 
    {
        // 1. Synchronize Filter State and Refresh Detail Grid
        var selectedCode = args.Row.GetCell("DetailCode").Value;
        selectionState.Text = selectedCode;
        Matrix.doRefresh("DetailGrid");

        // 2. Dynamic Metadata Injection
        // Check if the selected record defines custom attributes (VALUE1-10)
        var hasCustomMetadata = false;
        for (var k = 1; k <= 10; k++) {
            if (args.Row.GetValue("VALUE" + k) !== "") {
                hasCustomMetadata = true;
                break;
            }
        }

        // Apply dynamic headers (Captions) to the Detail Grid
        for (var i = 1; i <= 10; i++) {
            var fieldKey = "VALUE" + i;
            if (!hasCustomMetadata) {
                // Default: Inherit field names from Master Grid schema
                detailGrid.GetField(fieldKey).Caption = masterGrid.GetField(fieldKey).Caption;
            } else {
                // Dynamic: Map Detail Grid headers to the attribute values of the Master Record
                detailGrid.GetField(fieldKey).Caption = args.Row.GetValue(fieldKey);
            }
        }
        
        // Reflect UI changes
        detailGrid.Update();
    }
};
