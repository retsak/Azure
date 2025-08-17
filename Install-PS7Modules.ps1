# PowerShell 7 Script to Install Required Modules

# List of modules to install
$modules = @(
    "Az",
    "AzureCLI",
    "Az.Accounts",
    "Az.OperationalInsights",
    "Microsoft.Graph.Authentication",
    "Microsoft.Graph.Beta.DeviceManagement",
    "Microsoft.Graph.Beta.DeviceManagement.Administration",
    "Microsoft.Graph.Beta.DeviceManagement.Enrollment",
    "Microsoft.Graph.DeviceManagement",
    "Microsoft.Graph.DeviceManagement.Enrollment",
    "Microsoft.Graph.Groups",
    "Microsoft.Graph.Identity.DirectoryManagement",
    "Microsoft.Graph.Users"
)

# Ensure latest PowerShellGet is available
Write-Output "Updating PowerShellGet to latest version..."
Install-Module PowerShellGet -Force -Scope CurrentUser

# Loop through and install each module if not already installed
foreach ($module in $modules) {
    if (Get-Module -ListAvailable -Name $module) {
        Write-Output "✅ $module is already installed."
    } else {
        Write-Output "📦 Installing $module..."
        try {
            Install-Module -Name $module -Scope CurrentUser -Force -AllowClobber
            Write-Output "✅ Successfully installed $module."
        } catch {
            Write-Output "❌ Failed to install $module. Error: $_"
        }
    }
}

Write-Output "🎉 Module installation completed!"
