import axios, { AxiosResponse } from 'axios'

// create an axios instance
const service = axios.create({
  baseURL: import.meta.env.VITE_APP_DEV_WEB_URL, // url = base url + request url
  // withCredentials: true, // send cookies when cross-domain requests
  timeout: 60000, // request timeout
})

// request interceptor
service.interceptors.request.use(
  (config) => {
    // do something before request is sent
    return config
  },
  (error) => {
    // do something with request error
    return Promise.reject(error)
  }
)

// response interceptor
service.interceptors.response.use(
  /**
   * Determine the request status by custom code
   * Here is just an example
   * You can also judge the status by HTTP Status Code
   */
  (response: AxiosResponse) => {
    if (response.data) {
      switch (response.data.code) {
        case 503:
          // do something when code is 503
          break
      }
    }
    return response.data
  },
  (error) => {
    console.log('err' + error) // for debug
    // do something when error
    return Promise.reject(error)
  }
)

export default service
