<template>
  <main>
    Swagger
    <div id="swagger-ui" class="swagger-ui"> </div>
  </main>
</template>
<script lang="ts">
  import { SwaggerUIBundle, SwaggerUIStandalonePreset } from 'swagger-ui-dist'
  export default {
    name: 'ImportSwagger',
    data() {
      return {}
    },
    created() {},
    mounted() {
      SwaggerUIBundle({
        url: '/api/openapi.json',
        dom_id: '#swagger-ui',
        presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
        layout: 'StandaloneLayout'
      })
    }
  }
</script>
<style scoped>
  @import 'swagger-ui-dist/swagger-ui.css';
  html {
    box-sizing: border-box;
    overflow: -moz-scrollbars-vertical;
    overflow-y: scroll;
  }

  *,
  *:before,
  *:after {
    box-sizing: inherit;
  }

  body {
    margin: 0;
    background: #fafafa;
  }
</style>
