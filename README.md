# gulp-excelsheets2json
> Excel Sheets (XLSX/XLS) to json.


## Usage
First, install `gulp-excelsheets2json` as a development dependency:

```shell
> npm install --save-dev gulp-excelsheets2json
```

Then, add it to your `gulpfile.js`:

```javascript
var excelsheets2json = require('gulp-excelsheets2json');

gulp.task('copy', function() {
    gulp.src('config/**.xlsx')
        .pipe(excel2json({
            trace: true
        }))
        .pipe(gulp.dest('build'))
});
```


## API

### excelsheets2json([options])

#### options.trace
Type: `Boolean`

Default: `false`

Whether to log each file path while convert success.

## License
MIT &copy; flomair
