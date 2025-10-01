## Demo HTML

The purpose of the demo HTML is to create patterns that will then be used in the `template.html` file to create the final report.

The demo HTML is not intended to be a final report, but rather a way to visualize and test the patterns that will be used in the final report.

It has a number of dummy placeholder elements that are not used in the final report, but are useful for testing and visualizing the patterns.

## How do we enforce page breaks

```html
    <!-- Page Break -->
    <div class="page-break" aria-hidden="true"></div>
    <!-- Page Break Before this Section -->
    <div class="page-break-before" aria-hidden="true"></div>
    <!-- Page Break After this Section -->
    <div class="page-break-after" aria-hidden="true"></div>
```

## Icons

We have material icons available via the Google Fonts CDN.

```html
<link href="https://fonts.googleapis.com/icon?family=Material+Icons" rel="stylesheet">
```

To use them just drop a span tag with the appropriate class and icon name.

```html
<!-- Example Icon: The class is always "material-icons" and the icon name is the content -->
<span class="material-icons">face</span>
```

Icon lists and more documentation is available [HERE](https://fonts.google.com/icons?icon.query=face)

## Dummy elements 

We've created a few placeholder elements 

```html
  <div class="dummyImage" style="height: 300px;"></div>
  <div class="dummyChart" style="height: 300px;"></div>
  <div class="dummyMap" style="height: 300px;"></div>
```
