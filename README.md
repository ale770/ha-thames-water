# Thames Water Integration

[![hacs_badge](https://img.shields.io/badge/HACS-Default-orange.svg?style=for-the-badge)](https://github.com/custom-components/hacs)

[![ko-fi](https://ko-fi.com/img/githubbutton_sm.svg)](https://ko-fi.com/V7V71CVL48)

# Home Assistant Integration for Thames Water Consumption Data

This Home Assistant integration retrieves water consumption data from Thames Water using their API. It allows you to monitor your water usage directly from your Home Assistant setup without needing additional devices.

You need a **Thames Water Smart Meter**. The water consumption data provided by this integration is delayed by approximately three days or more. This delay is a characteristic of the Thames Water data system and cannot be altered in this integration.

The integration uses [ThamesWaterClient](https://github.com/AyrtonB/Thames-Water) python library to handle the authentication and getting the latest usage data.


## Installation

### Installation through HACS

1. Install the custom component using the Home Assistant Community Store (HACS) by adding the Custom Repository:
https://github.com/ale770/ha-thames-water
2. In the HACS panel, select Thames Water from the repository list and select the DOWNLOAD button.
3. Restart HA
4. Go to Settings > Devices & Services > Add Integration and select Thames Water.

### Manual installation

Copy the `custom_components/thames_water/` directory and all of its files to your `config/custom_components/` directory.

## Configuration

Once installed, restart Home Assistant:

[![Open your Home Assistant instance and show the system dashboard.](https://my.home-assistant.io/badges/system_dashboard.svg)](https://my.home-assistant.io/redirect/system_dashboard/)

Then, add the integration:

[![Open your Home Assistant instance and start setting up a new integration.](https://my.home-assistant.io/badges/config_flow_start.svg)](https://my.home-assistant.io/redirect/config_flow_start/?domain=thames_water)


<details>
  <summary>Manually add the Integration</summary>
  Visit the <i>Integrations</i> section in Home Assistant and click the <i>Add</i> button in the bottom right corner. Search for <code>Thames Water</code> and input your details. <b>You may need to clear your browser cache before the integration appears in the list.</b>
</details>

## Sensors

| Sensor | Description |
|---|---|
| **Total Reading** | Cumulative meter reading (latest value from the meter) |
| **Daily Usage** | Total water consumption for the latest available day |
| **Min Daily Flow** | Minimum hourly usage for the latest day — useful for detecting leaks |
| **Last Data Date** | Timestamp of the most recent data point received from Thames Water |

## Energy Management

The water statistics can be integrated into HA [Home Energy Management](https://www.home-assistant.io/docs/energy/) using **thames_water:thameswater_consumption**.

**thames_water:thameswater_cost** can be used to track costs.
The cost per litre can be configured in the device configuration page.
Changing this value will only affect new readings. If you want to update past readings you'll need to remove the integration, delete the statistics and add the integration again.

You can set at what time it will try and fetch new data using the fetch_data parameter.

[![Open your Home Assistant instance and show your Energy configuration panel.](https://my.home-assistant.io/badges/config_energy.svg)](https://my.home-assistant.io/redirect/config_energy/)

![Dashboard](./dashboard.png)
