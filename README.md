Simple experiments in doing data-science oriented tasks in the JVM/JS/native (from [Kotlin](https://kotlinlang.org/docs/reference/)).

Starting first with a simple self-contained Apache Spark hello world example (word count). Later, I will try out the
[TensorFlow Java API](https://www.tensorflow.org/api_docs/java/reference/org/tensorflow/package-summary), Kotlinx [coroutines](https://kotlinlang.org/docs/reference/coroutines/coroutines-guide.html) and perhaps multiplatform code.

Use either the bundled gradlew script or system-installed gradle (it will automatically find the version tied to this repo).

Run:

```sh
# launches whatever the buildfile points to as main
$ gradle run
```

Test:

```sh
# runs kotlin-test unit tests
$ gradle test
```

© 2018 Damien Stanton

See LICENSE for details.

[![Buy Me A Coffee](https://www.buymeacoffee.com/assets/img/custom_images/white_img.png)](https://www.buymeacoffee.com/damienstanton)
