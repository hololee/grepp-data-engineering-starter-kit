## Assignment1
서비스: 사용자의 갤러리에 들어있는 사진을 그룹으로 분류해주는 정리하는 서비스.

### raw_data  
- Table
  - users: 사용자의 계정 정보, 가입일자, 등등..
  - gallery_dirs: 특정 폴더 위치, 저장된 미디어 타입 (image, video), media id 등등...
  - gallery_shared: 공유 그룹 명, 공유 id, 권한, 제한 날짜 등등...
  - group_shared: 공유 id, 공유 할당 인원 정보, 가입 여부 등등...
  - users_images: 이미지 저장 위치, 할당 영상 분류 그룹 정보, 크기, 찍은날짜, 공유 여부 등등.. 
  - users_videos: 영상의 저장 위치. 할당 영상 분류 그룹 정, 영상 길이, 크기, 등등..

### analytics
  - top_categorized: 순위, 영상 분류 그룹 명, 분류 수 등등 -> 자동 분류시 할당되는 이미지나 영상의 수를 확인.
  - image_categorized: 이미지가 할당된 카테고리의 카운트 수를 비교
  - video_categorized: 영상이 할당된 카테고리의 카운트 수를 비교
  - shared_period_categorized: 각 카테고리별 평균 공유 기간을 비교
